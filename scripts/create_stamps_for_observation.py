#!/usr/bin/env python3

import os
import re
import sys
import argparse
from glob import glob
from getpass import getpass
from contextlib import suppress

from astropy.io import fits
from tqdm import tqdm

from piaa.utils import pipeline
from pocs.utils import current_time
from pocs.utils.images import fits as fits_utils
from pocs.utils.google import storage

import logging


def main(fits_files, stamp_size=(14, 14), snr_limit=10, *args, **kwargs):
    start_time = current_time()

    fits_files = files
    ext = 0
    if fits_files[0].endswith('.fz'):
        ext = 1
    sequence = fits.getval(fits_files[0], 'SEQID', ext=ext)

    unit_id, cam_id, seq_time = sequence.split('_')
    unit_id = re.match(r'.*(PAN\d\d\d).*', unit_id)[1]
    sequence = '_'.join([unit_id, cam_id, seq_time])

    data_dir =  os.path.dirname(fits_files[0])

    num_frames = len(fits_files)
    logger.info("Using sequence id {} with {} frames".format(sequence, num_frames))
    logger.info("Data directory: {}".format(data_dir))

    # Plate-solve all the images - safe to run again
    logger.info("Plate-solving all images")
    solved_files = list()
    for i, fn in enumerate(fits_files):
        try:
            fits_utils.get_solve_field(fn, timeout=90, verbose=True)
            solved_files.append(fn)
        except Exception as e:
            logger.info("Can't solve file {} {}".format(fn, e))
            continue

    logger.info("Looking up stars in field")
    # Lookup point sources
    point_sources = pipeline.lookup_point_sources(
        solved_files[0],
        force_new=True
    )

    logger.info("Sources found: {}".format(len(point_sources)))

    high_snr = point_sources[point_sources.snr >= float(snr_limit)]

    logger.info("Sources found w/ high SNR: {}".format(len(high_snr)))

    # Create stamps
    stamps_fn = pipeline.create_stamp_slices(
        data_dir,
        solved_files,
        high_snr,
        stamp_size=stamp_size,
        verbose=True,
        force_new=True
    )

    if stamps_fn:
        logger.info("Stamps file created: {}".format(stamps_fn))

    end_time = current_time()
    logger.info("Total time: {:.02f} seconds".format((end_time - start_time).sec))

    return stamps_fn


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create a PSC for each detected source.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--sequence', default=None, type=str,
            help="Sequence for the observation, will download if not available.")
    group.add_argument('--directory', default=None, type=str, help="Directory containing observation images.")
    parser.add_argument('--stamp-size', default=14, help="Square stamp size")
    parser.add_argument('--snr_limit', default=10, help="Detected SNR limit for creating stamps.")
    parser.add_argument('--log_level', default='debug', help="Log level")
    parser.add_argument( '--log_file', help="Log files, default $PANLOG/create_stamps_<datestamp>.log")

    args = parser.parse_args()

    ################ Setup logging ##############
    log_file = os.path.join(
        os.environ['PANDIR'],
        'logs',
        'per-run',
        'create_stamps_{}.log'.format(current_time(flatten=True))
    )
    common_log_path = os.path.join(
        os.environ['PANDIR'],
        'logs',
        'create_stamps.log'
    )

    if args.log_file:
        log_file = args.log_file

    try:
        log_level = getattr(logging, args.log_level.upper())
    except AttributeError:
        log_level = logging.DEBUG
    finally:
        logging.basicConfig(filename=log_file, level=log_level)

        with suppress(FileNotFoundError):
            os.remove(common_log_path)

        os.symlink(log_file, common_log_path)

        logger = logging.getLogger(__name__)

    logger.info('*' * 80)
    ################ End Setup logging ##############

    fits_files = None
    if args.directory is not None:
        fits_files = sorted(glob(os.path.join(
            args.directory,
            '2018*.fits*'
        ), recursive=True))

        logger.info("Found {} FITS files in {}".format(len(fits_files), args.directory))

    if args.sequence is not None:
        data_dir = '/var/panoptes/images/fields'

        # Download FITS files
        logger.info("Looking up FITS blobs")
        try:
            pan_storage = storage.PanStorage('panoptes-survey')
        except error.GoogleCloudError:
            logger.warning('Cannot connect to storage')
            sys.exit(1)

        logger.info("Have PanStorage instance: {}".format(pan_storage))
        fits_blobs = pan_storage.get_file_blobs(args.sequence)
        logger.info('Found {} blobs'.format(len(fits_blobs)))

        # Download all the FITS files from a bucket
        if fits_blobs:
            with tqdm(len(fits_blobs), 'Downloading FITS files'.ljust(25)) as bar:
                for i, blob in enumerate(fits_blobs):
                    fits_fn = pan_storage.download_file(blob, save_dir=data_dir, overwrite=True)
                    fits_files.append(fits_fn)
                    bar.update(i)

        fits_files = fits_files
        num_frames = len(fits_files)

    args.stamp_size = (args.stamp_size, args.stamp_size)

    stamps_fn = main(**vars(args), fits_files=fits_files)
    if stamps_fn:
        print("PSC file created: {}".format(stamps_fn))
