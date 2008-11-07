import BTrees.Length
from BTrees.IIBTree import intersection, weightedIntersection, IISet
from BTrees.OIBTree import OIBTree
from BTrees.IOBTree import IOBTree
from Products.ZCatalog.Lazy import LazyMap, LazyCat
import types

from DateTime import DateTime
from md5 import md5
import time
from Products.ZCatalog.Catalog import LOG
from os import environ

import transaction
from zope.interface import implements
from transaction.interfaces import IDataManager

try:
    import memcache
    s = environ.get('MEMCACHE_SERVERS', '')
    if s:
        servers = s.split(',')

    if not s:
        HAS_MEMCACHE = False
        LOG.info("No memcached servers defined. Catalog will function as normal.")

    else:
        mem_cache = memcache.Client(servers, debug=0)
        HAS_MEMCACHE = True
        LOG.info("Using memcached servers %s" % ",".join(servers))

except ImportError:
    mem_cache = None
    HAS_MEMCACHE = False
    LOG.info("Cannot import memcached. Catalog will function as normal.")

MEMCACHE_DURATION = 7200
MEMCACHE_RETRY_INTERVAL = 10
memcache_insertion_timestamps = {}
_hits = {}
_misses = {}
_memcache_failure_timestamp = 0
_cache_misses = {}

class MemcachedDataManager(object):

    implements(IDataManager)

    def __init__(self, id, cacheadapter, key_prefix='', to_set={}, to_delete=[], duration=None):
        self.id = id
        self.cacheadapter = cacheadapter
        self.key_prefix = key_prefix
        self.to_set = to_set
        self.to_delete = to_delete
        self.duration = duration

    def abort(self, trans):
        pass

    def commit(self, trans):
        pass

    def tpc_begin(self, trans):
        pass

    def tpc_vote(self, trans):
        self.cacheadapter.commit()        

    def tpc_finish(self, trans):
        pass

    def tpc_abort(self, trans):
        pass

    def sortKey(self):
        return 'MemcachedDataManager%s' % self.id 

class MemcachedAdapter(object):

    def __init__(self, memcache, default_duration):
        self.memcache = memcache
        self.default_duration = default_duration
        self.counter = 1000000        
        txn = transaction.get()
        txn.join(MemcachedDataManager(self.counter, self))

    def set_multi(self, to_set, key_prefix, duration=None, immediate=False):
        """
        Returns: 
            failure: (a) list of keys which failed to be stored or 
                     (b) False if no memcache servers could be reached
            success: empty list
        """
        global _memcache_failure_timestamp

        #LOG.debug("set multi (%s): %s" % (immediate, repr(to_set)))

        if immediate:
            # An edge case in the python memcache wrapper requires that
            # we catch TypeErrors.
            try:
                result = self.memcache.set_multi(to_set, key_prefix=key_prefix, time=duration or self.default_duration)
            except TypeError:
                return False

            # xxx: I hate this code here. Make it a callback so transaction commit
            # can set _memcache_failure_timestamp
            # Return value of non-empty list indicates error
            if isinstance(result, types.ListType) and len(result):
                LOG.error("_cache_result set_multi failed") 
                _memcache_failure_timestamp = int(time.time())
                # The return value of set_multi is the original to_set list in 
                # case of no daemons responding.
                if len(result) != len(to_set.keys()):
                    LOG.error("Some keys were successfully written to memcache. This case needs further handling.")

            return result

        txn = transaction.get()
        self.counter += 1
        if not hasattr(txn, 'v_cache'):
            txn.v_cache = dict()
        for k,v in to_set.items():
            s_k = key_prefix + str(k)
            # Add to v_cache
            txn.v_cache[s_k] = v
            # Remove from v_delete_cache
            if hasattr(txn, 'v_delete_cache') and (s_k in txn.v_delete_cache):            
                try:
                    txn.v_delete_cache.remove(s_k)
                except ValueError:
                    pass

    def get(self, key, default=[]):
        """
        Parameter key is already prefixed

        Returns: 
            success: value
            failure: default
        """
        txn = transaction.get()

        if hasattr(txn, 'v_delete_cache') and (key in txn.v_delete_cache):
            return default

        if hasattr(txn, 'v_cache') and txn.v_cache.has_key(key):            
            try:
                return txn.v_cache[key]
            except KeyError:
                pass

        result = self.memcache.get(key)
        if result is None:
            return default
        return result

    def get_multi(self, to_get, key_prefix):
        """
        Returns: 
            success, failure: dictionary
        """
        if not to_get:
            # Nothing to do
            return {}

        txn = transaction.get()       

        # If any key is in v_delete_cache then to_get must be adjusted
        new_to_get = []
        if hasattr(txn, 'v_delete_cache'):
            for k in to_get:
                s_k = key_prefix + str(k)
                if s_k not in txn.v_delete_cache:
                    new_to_get.append(k)
        else:
            new_to_get = to_get

        result_cache = {}
        keys_still_to_get = []
        # Try and find the keys in v_cache
        if hasattr(txn, 'v_cache'):            
            for k in new_to_get:
                s_k = key_prefix + str(k)
                if txn.v_cache.has_key(s_k):
                    try:
                        result_cache[k] = txn.v_cache[s_k]                         
                    except KeyError:
                        keys_still_to_get.append(k)
                else:
                    keys_still_to_get.append(k)
        else:
            keys_still_to_get = new_to_get
       
        result_memcache = {} 
        if keys_still_to_get:
            # An edge case in the python memcache wrapper requires that
            # we catch KeyErrors.
            try:        
                result_memcache = self.memcache.get_multi(keys_still_to_get, key_prefix=key_prefix)
            except KeyError:
                pass

        # Collate the result sets
        result_memcache.update(result_cache)

        return result_memcache

    def delete_multi(self, to_delete, immediate=False):        
        """
        All elements in to_delete are already prefixed

        Returns:
            success: 1
            failure: not 1
        """
        if not to_delete:
            # Nothing to do
            return 1

        #LOG.debug("delete multi (%s): %s" % (immediate, repr(to_delete)))

        if immediate:
            return self.memcache.delete_multi(to_delete)

        txn = transaction.get()

        if not hasattr(txn, 'v_delete_cache'):
            txn.v_delete_cache = []
        for k in to_delete:
            if k not in txn.v_delete_cache:
                txn.v_delete_cache.append(k)

        self.counter += 1
        if hasattr(txn, 'v_cache'):
            for k in to_delete:
                if txn.v_cache.has_key(k):
                    try:
                        del txn.v_cache[k]
                    except KeyError:
                        pass

        return 1

    def flush_all(self):
        """
        Returns:
            success, failure: undefined
        """
        txn = transaction.get()
        if hasattr(txn, 'v_cache'):
            txn.v_cache.clear()
        if hasattr(txn, 'v_cache'):
            txn.v_delete_cache = []
        return self.memcache.flush_all()

    def commit(self):
        """
        Do one atomic commit. This is in fact not atomic since the memcached wrapper
        needs more work but it is the best we can do.
        """
        txn = transaction.get()
        if hasattr(txn, 'v_delete_cache'):
            if self.delete_multi(to_delete=txn.v_delete_cache, immediate=True) != 1:
                LOG.error("_invalidate_cache delete_multi failed")
            txn.v_delete_cache = []

        if hasattr(txn, 'v_cache'):
            result_set = self.set_multi(to_set=txn.v_cache, 
                key_prefix='', 
                duration=self.default_duration, 
                immediate=True)
            txn.v_cache.clear()            
            # Error logging is handled by the set_multi method

        # xxx: consider what to do in case of failures

def _getMemcachedAdapter(self):
    global mem_cache, MEMCACHE_DURATION
    txn = transaction.get()
    if not hasattr(txn, 'v_memcached_adapter'):
        txn.v_memcached_adapter = MemcachedAdapter(mem_cache, default_duration=MEMCACHE_DURATION) 
    return txn.v_memcached_adapter

def _memcache_available(self):
    global HAS_MEMCACHE, MEMCACHE_RETRY_INTERVAL, _memcache_failure_timestamp
    if not HAS_MEMCACHE:
        return False

    now_seconds = int(time.time())
    if now_seconds - _memcache_failure_timestamp < MEMCACHE_RETRY_INTERVAL:
        return False

    _memcache_failure_timestamp = 0
    return True

def _cache_result(self, cache_key, rs, search_indexes=[]):
    global MEMCACHE_DURATION,  _memcache_failure_timestamp

    if not self._memcache_available():
        return

    # Insane case. This only happens when search returns everything 
    # in the catalog. Naturally we avoid this.
    if rs is None:
        return

    cache_id = '/'.join(self.getPhysicalPath())
    to_set = {}

    lcache_key = cache_id + cache_key
    to_set[cache_key] = rs

    # Use get_multi with a prefix to save bandwidth
    to_get = []
    for r in rs:
        to_get.append(str(r))
        to_set[str(r)] = [lcache_key]
    for idx in search_indexes:
        if idx in ('sort_on','sort_order','sort_limit'): continue  
        to_get.append(idx)
        to_set[idx] = [lcache_key]

    # Augment the values of to_set with possibly existing values
    result = self._getMemcachedAdapter().get_multi(to_get, key_prefix=cache_id)
    for k,v in result.items():
        if not isinstance(v, types.ListType): continue
        to_set[k].extend(v)

    if to_set:
        now_seconds = int(time.time())

        # During a large number of new queries (and hence new calls to this method)
        # we may try to set the same to_set in memcache over and over again, and
        # all of them will timeout in the python memcache wrapper. The inserts will 
        # probably still take place, but it will be the same to_set applied many
        # times. This is obviously redundant and will cause memcache to consume too
        # much CPU. 
        # To overcome this we abide by this rule: you cannot insert the same set more 
        # than once every N seconds.           
        li = to_set.items()
        li.sort()
        hash = md5(str(li)).hexdigest()
        if (now_seconds - memcache_insertion_timestamps.get(hash, 0)) < 10:
            LOG.debug("Prevent a call to set_multi since the same insert was done recently")
            return
        memcache_insertion_timestamps[hash] = now_seconds                       

        result = self._getMemcachedAdapter().set_multi(to_set, key_prefix=cache_id, duration=MEMCACHE_DURATION)
        if result == False:
            return

        '''
        # xxx: restore later
        # Return value of non-empty list indicates error
        if isinstance(result, types.ListType) and len(result):
            LOG.error("_cache_result set_multi failed") 
            _memcache_failure_timestamp = now_seconds
            # The return value of set_multi is the original to_set list in 
            # case of no daemons responding.
            if len(result) != len(to_set):
                LOG.error("Some keys were successfully written to memcache. This case needs further handling.")
                # xxx: maybe do a self._clear_cache()?
        '''

def _get_cached_result(self, cache_key, default=[]):
    global _memcache_failure_timestamp

    if not self._memcache_available():
        return default

    cache_id = '/'.join(self.getPhysicalPath())
    key = cache_id + cache_key
    _cache_misses.setdefault(key, 0)
    result = self._getMemcachedAdapter().get(key, default)
    # todo: Return default if any item in rs is not an integer. How?        
    if result is None:
        # Record the time of the miss. If we keep missing this key
        # then something is wrong with memcache and we must stop
        # hitting it for a while.
        now_seconds = int(time.time())           
        if _cache_misses.get(key, 0) > 10:
            LOG.error("_get_cache_key failed 10 times") 
            _memcache_failure_timestamp = now_seconds
            _cache_misses.clear()
        else:
            try:
                _cache_misses[key] += 1
            except KeyError:
                pass
        return default

    _cache_misses[key] = 0  
    return result

def _invalidate_cache(self, rid=None, index_name='', immediate=False):
    """ Invalidate cached results affected by rid and / or index_name
    """
    global _memcache_failure_timestamp

    if not self._memcache_available():
        return

    cache_id = '/'.join(self.getPhysicalPath())
    LOG.debug('[%s] _invalidate_cache rid=%s, index_name=%s' % (cache_id, rid, index_name))

    to_delete = []

    # rid and index_name are mutually exclusive, so no need for get_multi
    # trickery.

    if rid is not None:
        s_rid = cache_id + str(rid)
        rid_map = self._getMemcachedAdapter().get(s_rid, [])        
        if rid_map is not None:
            to_delete.extend(rid_map)
        to_delete.append(s_rid)

    if index_name:
        s_index_name = cache_id + index_name
        index_map = self._getMemcachedAdapter().get(s_index_name, []) 
        if index_map is not None:
            to_delete.extend(index_map)
        to_delete.append(s_index_name)

    if to_delete:
        now_seconds = int(time.time())
        LOG.debug('[%s] Remove %s items from cache' % (cache_id, len(to_delete)))
        # Return value of 1 indicates no error
        if self._getMemcachedAdapter().delete_multi(to_delete, immediate=immediate) != 1:
            LOG.error("_invalidate_cache delete_multi failed")
            _memcache_failure_timestamp = now_seconds

def _clear_cache(self):  
    if not self._memcache_available():
        return
    LOG.debug('Flush cache')
    # No return value for flush_all
    # xxx: This flushes all caches which is inefficient. Currently
    # there is no way to delete all keys starting with eg. 
    # /site/portal_catalog
    self._getMemcachedAdapter().flush_all()
    _hits.clear()
    _misses.clear()

def _get_cache_key(self, args):

    def pin_datetime(dt):
        # Pin to dt granularity which is 1 minute by default
        return dt.strftime('%Y-%m-%d.%h:%m %Z')

    items = list(args.request.items())
    items.extend(list(args.keywords.items()))
    items.sort()
    sorted = []
    for k, v in items:
        if isinstance(v, types.ListType):
            v.sort()

        elif isinstance(v, types.TupleType):
            v = list(v)
            v.sort()

        elif isinstance(v, DateTime):
            v = pin_datetime(v)

        elif isinstance(v, types.DictType):               
            # Find DateTime objects in v and pin them
            tsorted = []
            titems = v.items()
            titems.sort()
            for tk, tv in titems:
                if isinstance(tv, DateTime):
                    tv = pin_datetime(tv)
                elif isinstance(tv, types.ListType) or isinstance(tv, types.TupleType):
                    li = []
                    for item in list(tv):
                        if isinstance(item, DateTime):
                            item = pin_datetime(item)
                        li.append(item)
                    tv = li

                tsorted.append((tk, tv))
            v = tsorted

        sorted.append((k,v))
    cache_key = str(sorted)
    return md5(cache_key).hexdigest()

def _get_search_indexes(self, args):
    keys = list(args.request.keys())
    keys.extend(list(args.keywords.keys()))
    return keys

# Methods clear, catalog, uncatalogObject, search are from the default Catalog.py
def clear(self):
    """ clear catalog """

    self._clear_cache()
    self.data  = IOBTree()  # mapping of rid to meta_data
    self.uids  = OIBTree()  # mapping of uid to rid
    self.paths = IOBTree()  # mapping of rid to uid
    self._length = BTrees.Length.Length()

    for index in self.indexes.keys():
        self.getIndex(index).clear()

def catalogObject(self, object, uid, threshold=None, idxs=None,
                  update_metadata=1):
    """
    Adds an object to the Catalog by iteratively applying it to
    all indexes.

    'object' is the object to be cataloged

    'uid' is the unique Catalog identifier for this object

    If 'idxs' is specified (as a sequence), apply the object only
    to the named indexes.

    If 'update_metadata' is true (the default), also update metadata for
    the object.  If the object is new to the catalog, this flag has
    no effect (metadata is always created for new objects).

    """        
    if idxs is None:
        idxs = []

    data = self.data
    index = self.uids.get(uid, None)

    if index is not None:
        self._invalidate_cache(rid=index)

    if index is None:  # we are inserting new data
        #self._clear_cache() # not needed? 
        index = self.updateMetadata(object, uid)

        if not hasattr(self, '_length'):
            self.migrate__len__()
        self._length.change(1)
        self.uids[uid] = index
        self.paths[index] = uid

    elif update_metadata:  # we are updating and we need to update metadata
        self.updateMetadata(object, uid)

    # do indexing

    total = 0

    if idxs==[]: use_indexes = self.indexes.keys()
    else:        use_indexes = idxs

    for name in use_indexes:
        x = self.getIndex(name)
        if hasattr(x, 'index_object'):
            before = self.getIndex(name).getEntryForObject(index, "")

            blah = x.index_object(index, object, threshold)

            after = self.getIndex(name).getEntryForObject(index, "")

            # If index has changed we must invalidate parts of the cache
            if before != after:
                self._invalidate_cache(index_name=name)

            total = total + blah
        else:
            LOG.error('catalogObject was passed bad index object %s.' % str(x))

    return total

def uncatalogObject(self, uid):
    """
    Uncatalog and object from the Catalog.  and 'uid' is a unique
    Catalog identifier

    Note, the uid must be the same as when the object was
    catalogued, otherwise it will not get removed from the catalog

    This method should not raise an exception if the uid cannot
    be found in the catalog.

    """
    data = self.data
    uids = self.uids
    paths = self.paths
    indexes = self.indexes.keys()
    rid = uids.get(uid, None)

    if rid is not None:
        self._invalidate_cache(rid=rid)

        for name in indexes:
            x = self.getIndex(name)
            if hasattr(x, 'unindex_object'):
                x.unindex_object(rid)
        del data[rid]
        del paths[rid]
        del uids[uid]
        if not hasattr(self, '_length'):
            self.migrate__len__()
        self._length.change(-1)
        
    else:
        LOG.error('uncatalogObject unsuccessfully '
                  'attempted to uncatalog an object '
                  'with a uid of %s. ' % str(uid))

def search(self, request, sort_index=None, reverse=0, limit=None, merge=1):
    """Iterate through the indexes, applying the query to each one. If
    merge is true then return a lazy result set (sorted if appropriate)
    otherwise return the raw (possibly scored) results for later merging.
    Limit is used in conjuntion with sorting or scored results to inform
    the catalog how many results you are really interested in. The catalog
    can then use optimizations to save time and memory. The number of
    results is not guaranteed to fall within the limit however, you should
    still slice or batch the results as usual."""

    rs = None # resultset

    # Indexes fulfill a fairly large contract here. We hand each
    # index the request mapping we are given (which may be composed
    # of some combination of web request, kw mappings or plain old dicts)
    # and the index decides what to do with it. If the index finds work
    # for itself in the request, it returns the results and a tuple of
    # the attributes that were used. If the index finds nothing for it
    # to do then it returns None.

    # For hysterical reasons, if all indexes return None for a given
    # request (and no attributes were used) then we append all results
    # in the Catalog. This generally happens when the search values
    # in request are all empty strings or do not coorespond to any of
    # the indexes.

    # Note that if the indexes find query arguments, but the end result
    # is an empty sequence, we do nothing
    cache_id = '/'.join(self.getPhysicalPath())
    cache_key = self._get_cache_key(request)
    _misses.setdefault(cache_id, 0)
    _hits.setdefault(cache_id, 0)
    marker = '_marker'
    rs = self._get_cached_result(cache_key, marker)

    if rs is marker:
        LOG.debug('[%s] MISS: %s' % (cache_id, cache_key)) 
        rs = None
        for i in self.indexes.keys():
            index = self.getIndex(i)
            _apply_index = getattr(index, "_apply_index", None)
            if _apply_index is None:
                continue
            r = _apply_index(request)

            if r is not None:
                r, u = r
                w, rs = weightedIntersection(rs, r)

        search_indexes = self._get_search_indexes(request)
        LOG.debug("[%s] Search indexes = %s" % (cache_id, str(search_indexes)))
        self._cache_result(cache_key, rs, search_indexes)

        try:
            _misses[cache_id] += 1
        except KeyError:
            pass
    else:
        #LOG.debug('[%s] HIT: %s' % (cache_id, cache_key)) 
        try:
            _hits[cache_id] += 1
        except KeyError:
            pass

    # Output stats
    if int(time.time()) % 10 == 0:
        hits = _hits.get(cache_id)
        if hits:
            misses = _misses.get(cache_id, 0)
            LOG.info('[%s] Hit rate: %.2f%%' % (cache_id, hits*100.0/(hits+misses)))

    if rs is None:
        # None of the indexes found anything to do with the request
        # We take this to mean that the query was empty (an empty filter)
        # and so we return everything in the catalog
        if sort_index is None:
            return LazyMap(self.instantiate, self.data.items(), len(self))
        else:
            return self.sortResults(
                self.data, sort_index, reverse,  limit, merge)
    elif rs:
        # We got some results from the indexes.
        # Sort and convert to sequences.
        # XXX: The check for 'values' is really stupid since we call
        # items() and *not* values()
        if sort_index is None and hasattr(rs, 'values'):
            # having a 'values' means we have a data structure with
            # scores.  Build a new result set, sort it by score, reverse
            # it, compute the normalized score, and Lazify it.
                            
            if not merge:
                # Don't bother to sort here, return a list of 
                # three tuples to be passed later to mergeResults
                # note that data_record_normalized_score_ cannot be
                # calculated and will always be 1 in this case
                getitem = self.__getitem__
                return [(score, (1, score, rid), getitem) 
                        for rid, score in rs.items()]
            
            rs = rs.byValue(0) # sort it by score
            max = float(rs[0][0])

            # Here we define our getter function inline so that
            # we can conveniently store the max value as a default arg
            # and make the normalized score computation lazy
            def getScoredResult(item, max=max, self=self):
                """
                Returns instances of self._v_brains, or whatever is passed
                into self.useBrains.
                """
                score, key = item
                r=self._v_result_class(self.data[key])\
                      .__of__(self.aq_parent)
                r.data_record_id_ = key
                r.data_record_score_ = score
                r.data_record_normalized_score_ = int(100. * score / max)
                return r
            
            return LazyMap(getScoredResult, rs, len(rs))

        elif sort_index is None and not hasattr(rs, 'values'):
            # no scores
            if hasattr(rs, 'keys'):
                rs = rs.keys()
            return LazyMap(self.__getitem__, rs, len(rs))
        else:
            # sort.  If there are scores, then this block is not
            # reached, therefore 'sort-on' does not happen in the
            # context of a text index query.  This should probably
            # sort by relevance first, then the 'sort-on' attribute.
            return self.sortResults(rs, sort_index, reverse, limit, merge)
    else:
        # Empty result set
        return LazyCat([])

def __getitem__(self, index, ttype=type(())):
    """
    Returns instances of self._v_brains, or whatever is passed
    into self.useBrains.
    """
    if type(index) is ttype:
        # then it contains a score...
        normalized_score, score, key = index
        # Memcache may be responsible for this bad key. Invalidate the
        # cache and let the exception take place. This should never be
        # needed since we started using transaction aware caching.
        if not isinstance(key, types.IntType) or not self.data.has_key(key):
            LOG.error("Weighted rid %s leads to KeyError. Removing from cache." % index)
            self._invalidate_cache(rid=index, immediate=True)
        r=self._v_result_class(self.data[key]).__of__(self.aq_parent)
        r.data_record_id_ = key
        r.data_record_score_ = score
        r.data_record_normalized_score_ = normalized_score
    else:
        # otherwise no score, set all scores to 1
        if not isinstance(index, types.IntType) or not self.data.has_key(index):
            LOG.error("rid %s leads to KeyError. Removing from cache." % index)
            self._invalidate_cache(rid=index, immediate=True)
        r=self._v_result_class(self.data[index]).__of__(self.aq_parent)
        r.data_record_id_ = index
        r.data_record_score_ = 1
        r.data_record_normalized_score_ = 1
    return r

from Products.ZCatalog.Catalog import Catalog
Catalog._getMemcachedAdapter = _getMemcachedAdapter
Catalog._memcache_available = _memcache_available
Catalog._cache_result = _cache_result
Catalog._get_cached_result = _get_cached_result
Catalog._invalidate_cache = _invalidate_cache
Catalog._clear_cache = _clear_cache
Catalog._get_cache_key = _get_cache_key
Catalog._get_search_indexes = _get_search_indexes
Catalog.clear = clear
Catalog.catalogObject = catalogObject
Catalog.uncatalogObject = uncatalogObject
Catalog.search = search
Catalog.__getitem__ = __getitem__
