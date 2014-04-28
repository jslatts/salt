# -*- coding: utf-8 -*-
'''
'''

# Import python libs
import logging
import os
import time
import hashlib
import pickle
import urllib
from copy import deepcopy

from pprint import pprint

# Import salt libs
from salt.pillar import Pillar
import salt.utils
import salt.utils.s3 as s3

# Set up logging
log = logging.getLogger(__name__)

# Define the module's virtual name
#__virtualname__ = 's3'

_s3_cache_expire = 30  # cache for 30 seconds
_s3_sync_on_update = True  # sync cache on update rather than jit


def __init__( __opts__ ):
    '''
    Initialize the local cache directory
    '''

    log.debug('Initializing S3 Pillar Cache')

    cache_dir = _get_cache_dir()

    if not os.path.isdir(cache_dir):
        os.makedirs(cache_dir)


def ext_pillar(minion_id, pillar, bucket, root=''):
    '''
    Execute a command and read the output as YAML
    '''
    branch = 'base'

    log.debug('ext_pillar called: Bucket is: %s, root is %s', bucket, root)

    # normpath is needed to remove appended '/' if root is empty string.
    pillar_dir = os.path.normpath(os.path.join(_get_cache_dir(), root))

    if __opts__['pillar_roots'].get(branch, []) == [pillar_dir]:
        return {}

    metadata = _init()

    if _s3_sync_on_update:
        key, keyid, service_url = _get_s3_key()

        # sync the buckets to the local cache
        log.info('Syncing local pillar cache from S3...')
        for saltenv, env_meta in metadata.iteritems():
            for bucket, files in _find_files(env_meta).iteritems():
                for file_path in files:
                    cached_file_path = _get_cached_file_name(bucket, saltenv, file_path)
                    log.info('{0} - {1} : {2}'.format(bucket, saltenv, file_path))

                    # load the file from S3 if it's not in the cache or it's old
                    _get_file_from_s3(metadata, saltenv, bucket, file_path, cached_file_path)

        log.info('Sync local pillar cache from S3 completed.')


    opts = deepcopy(__opts__)

    opts['pillar_roots'][branch] = [pillar_dir]

    pil = Pillar(opts, __grains__, minion_id, branch)

    compiled_pillar = pil.compile_pillar()

    pprint(opts)
    pprint(compiled_pillar)

    return compiled_pillar


def _get_s3_key():
    '''
    Get AWS keys from pillar or config
    '''

    key = __opts__['s3.key'] if 's3.key' in __opts__ else None
    keyid = __opts__['s3.keyid'] if 's3.keyid' in __opts__ else None
    service_url = __opts__['s3.service_url'] \
        if 's3.service_url' in __opts__ \
        else None

    return key, keyid, service_url


def _init():
    '''
    Connect to S3 and download the metadata for each file in all buckets
    specified and cache the data to disk.
    '''

    cache_file = _get_buckets_cache_filename()
    exp = time.time() - _s3_cache_expire

    # check mtime of the buckets files cache
    if os.path.isfile(cache_file) and os.path.getmtime(cache_file) > exp:
        return _read_buckets_cache_file(cache_file)
    else:
        # bucket files cache expired
        return _refresh_buckets_cache_file(cache_file)


def _get_cache_dir():
    '''
    Get pillar cache directory
    '''

    return os.path.join(__opts__['cachedir'], 'pillar_s3fs')


def _get_cached_file_name(bucket_name, saltenv, path):
    '''
    Return the cached file name for a bucket path file
    '''

    file_path = os.path.join(_get_cache_dir(), saltenv, bucket_name, path)

    # make sure bucket and saltenv directories exist
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    return file_path


def _get_buckets_cache_filename():
    '''
    Return the filename of the cache for bucket contents.
    Create the path if it does not exist.
    '''

    cache_dir = _get_cache_dir()
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return os.path.join(cache_dir, 'buckets_files.cache')


def _refresh_buckets_cache_file(cache_file):
    '''
    Retrieve the content of all buckets and cache the metadata to the buckets
    cache file
    '''

    log.debug('Refreshing S3 buckets pillar cache file')

    key, keyid, service_url = _get_s3_key()
    metadata = {}

    # helper s3 query function
    def __get_s3_meta(bucket, key=key, keyid=keyid):
        return s3.query(
                key=key,
                keyid=keyid,
                bucket=bucket,
                service_url=service_url,
                return_bin=False)

    if _is_env_per_bucket():
        # Single environment per bucket
        for saltenv, buckets in _get_buckets().items():
            bucket_files = {}
            for bucket_name in buckets:
                s3_meta = __get_s3_meta(bucket_name)

                # s3 query returned nothing
                if not s3_meta:
                    continue

                # grab only the files/dirs
                bucket_files[bucket_name] = filter(lambda k: 'Key' in k, s3_meta)

            metadata[saltenv] = bucket_files

    else:
        # Multiple environments per buckets
        for bucket_name in _get_buckets():
            s3_meta = __get_s3_meta(bucket_name)

            # s3 query returned nothing
            if not s3_meta:
                continue

            # pull out the environment dirs (eg. the root dirs)
            files = filter(lambda k: 'Key' in k, s3_meta)
            environments = map(lambda k: (os.path.dirname(k['Key']).split('/', 1))[0], files)
            environments = set(environments)

            # pull out the files for the environment
            for saltenv in environments:
                # grab only files/dirs that match this saltenv
                env_files = filter(lambda k: k['Key'].startswith(saltenv), files)

                if saltenv not in metadata:
                    metadata[saltenv] = {}

                if bucket_name not in metadata[saltenv]:
                    metadata[saltenv][bucket_name] = []

                metadata[saltenv][bucket_name] += env_files

    # write the metadata to disk
    if os.path.isfile(cache_file):
        os.remove(cache_file)

    log.debug('Writing S3 buckets pillar cache file')

    with salt.utils.fopen(cache_file, 'w') as fp_:
        pickle.dump(metadata, fp_)

    pprint(metadata)

    return metadata


def _read_buckets_cache_file(cache_file):
    '''
    Return the contents of the buckets cache file
    '''

    log.debug('Reading buckets cache file')

    with salt.utils.fopen(cache_file, 'rb') as fp_:
        data = pickle.load(fp_)

    return data


def _find_files(metadata, dirs_only=False):
    '''
    Looks for all the files in the S3 bucket cache metadata
    '''

    ret = {}

    for bucket_name, data in metadata.iteritems():
        if bucket_name not in ret:
            ret[bucket_name] = []

        # grab the paths from the metadata
        filePaths = map(lambda k: k['Key'], data)
        # filter out the files or the dirs depending on flag
        ret[bucket_name] += filter(lambda k: k.endswith('/') == dirs_only, filePaths)

    return ret


def _find_file_meta(metadata, bucket_name, saltenv, path):
    '''
    Looks for a file's metadata in the S3 bucket cache file
    '''

    env_meta = metadata[saltenv] if saltenv in metadata else {}
    bucket_meta = env_meta[bucket_name] if bucket_name in env_meta else {}
    files_meta = filter((lambda k: 'Key' in k), bucket_meta)

    for item_meta in files_meta:
        if 'Key' in item_meta and item_meta['Key'] == path:
            return item_meta


def _get_buckets():
    '''
    Return the configuration buckets
    '''

    return __opts__['s3.buckets'] if 's3.buckets' in __opts__ else {}


def _get_file_from_s3(metadata, saltenv, bucket_name, path, cached_file_path):
    '''
    Checks the local cache for the file, if it's old or missing go grab the
    file from S3 and update the cache
    '''

    # check the local cache...
    if os.path.isfile(cached_file_path):
        file_meta = _find_file_meta(metadata, bucket_name, saltenv, path)
        file_md5 = filter(str.isalnum, file_meta['ETag']) if file_meta else None

        cached_file_hash = hashlib.md5()
        with salt.utils.fopen(cached_file_path, 'rb') as fp_:
            cached_file_hash.update(fp_.read())

        # hashes match we have a cache hit
        if cached_file_hash.hexdigest() == file_md5:
            return

    # ... or get the file from S3
    key, keyid, service_url = _get_s3_key()
    s3.query(
        key=key,
        keyid=keyid,
        bucket=bucket_name,
        service_url=service_url,
        path=urllib.quote(path),
        local_file=cached_file_path
    )


def _trim_env_off_path(paths, saltenv, trim_slash=False):
    '''
    Return a list of file paths with the saltenv directory removed
    '''
    env_len = None if _is_env_per_bucket() else len(saltenv) + 1
    slash_len = -1 if trim_slash else None

    return map(lambda d: d[env_len:slash_len], paths)


def _is_env_per_bucket():
    '''
    Return the configuration mode, either buckets per environment or a list of
    buckets that have environment dirs in their root
    '''

    buckets = _get_buckets()
    if isinstance(buckets, dict):
        return True
    elif isinstance(buckets, list):
        return False
    else:
        raise ValueError('Incorrect s3.buckets type given in config')
