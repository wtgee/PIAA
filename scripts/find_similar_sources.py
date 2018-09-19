#!/usr/bin/env python3

import os
import numpy as np
import h5py                                   
from glob import glob
from tqdm import tqdm
from contextlib import suppress

from piaa.utils import pipeline
from pocs.utils import current_time

import logging

def main(stamp_file, picid=None, show_progress=True, force=False, *args, **kwargs):
    try:
        stamps = h5py.File(stamp_file)
    except FileNotFoundError:
        logging.warning("File not found: {}".format(stamp_file))
        return
    
    if picid:
        star_iterator = enumerate([picid])
        total = 1
    else:
        star_iterator = enumerate(list(stamps.keys()))
        total = len(list(stamps.keys()))
    
    # Show progress bar
    if show_progress:
        star_iterator = tqdm(star_iterator, total=total, desc='Looping sources')

    for i, source_picid in star_iterator:
        if force is False and 'similar_stars' in stamps[source_picid]:
            logging.debug("Skipping {} - already exists".format(source_picid))
            continue

        diff = list()
        flags = stamps[source_picid].attrs['flags']

        if int(flags) and int(flags) != 2:
            logging.debug("Skipping {} - SE flags: {}".format(source_picid, flags))
            continue

        if float(stamps[source_picid].attrs['vmag']) > 13:
            logging.debug("Skipping {} - Vmag: {:.02f} > 13".format(
                source_picid, 
                float(stamps[source_picid].attrs['vmag']))
            )
            continue

        local_csv_file = stamp_file.replace('.hdf5', '_{}.csv'.format(picid))
        vary_series = pipeline.find_similar_stars(
            picid, 
            stamps, 
            show_progress=show_progress,
            csv_file=local_csv_file,
            force_new=force
        )
        
        #top_index = [int(x) for x in list(vary_series[:200].index)]
        #
        #if force:
        #    try:
        #        del stamps[source_picid]['similar_stars']
        #        del stamps[source_picid]['similar_star_scores']
        #    except KeyError:
        #        pass
        
        ## Store in stamps file
        #logging.info("Success {}".format(source_picid))
        #stamps[source_picid]['similar_stars'] = top_index
        #stamps[source_picid]['similar_star_scores'] = np.array(vary_series[:200])
        #stamps.flush()

        
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Find morphologically similar stars")
    parser.add_argument('--stamp_file', required=True, type=str, help="HDF5 Stamps file")
    parser.add_argument('--csv_file', type=str, help="Filename for saved csv")
    parser.add_argument('--picid', default=None, help="Only perform search for given PICID. Otherwise use all sources.")
    parser.add_argument('--log_level', default='debug', help="Log level")
    parser.add_argument('--log_file', help="Log files, default $PANLOG/create_stamps_<datestamp>.log")
    parser.add_argument('--show_progress', default=True, action='store_true', 
                        help="Show progress bars")
    parser.add_argument('--force', default=False, action='store_true', help='Force new entries')
    
    args = parser.parse_args()
    
    ################ Setup logging ##############
    log_file = os.path.join(
        os.environ['PANDIR'], 
        'logs', 
        'per-run',
        'find_similar_sources_{}.log'.format(current_time(flatten=True))
    )
    common_log_path = os.path.join(
        os.environ['PANDIR'],
        'logs',
        'find_similar_sources.log'
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
        
    logging.info('*'*80)
    ################ End Setup logging ##############
    
    csv_out = main(**vars(args))
    if csv_out:
        print("Similar sources added to {}".format(csv_out))
