Introduction
============
collective.catalogcache uses memcached to cache ZCatalog search results. 

Caching is transparent. Invalidation code is called at appropriate places to ensure that the state of the cached results remains consistent.

The use of memcached enables the safe use of caching in a distributed environment.

***  WARNING ***
collective.catalogcache is a big plaster to put over your site while you determine why things are slow. Use in production is recommended for the brave / foolhardy.

Requirements
============
Zope 2.9.6 - Zope 2.10.6. Other versions are possibly supported but not tested. 
memcached. Any recent version should work. Download from http://www.danga.com/memcached.
python-memcached. Download from http://pypi.python.org/pypi/python-memcached.

If memcached or python-memcached is not available the catalog will function as usual.

Installation
============
A buildout is provided at http://dev.plone.org/collective/browser/collective.catalogcache/trunk/buildout.cfg. The buildout is for Plone 3, but the product can be used with plain Zope.

To enable collective.kssinline for an existing installation ensure that the following is present in your buildout.cfg

In the eggs section collective.catalogcache

In the zcml section collective.catalogcache

You must edit your zope.conf to declare eg.
<environment>
    MEMCACHE_SERVERS 127.0.0.1:11211,127.0.0.1:11212
</environment>

You *must* ensure that different logical Zope instances do not use the same memcache servers. Naturally Zeo clients *must* use the same set of servers.

Run ./bin/buildout -Nv and restart your instance.

Notes on memcached 
==================
memcached is designed to run in a distributed environment, hence it is a good idea to run at least two instances on a single machine. More is possibly better depending on your hardware.

To start up two memcached instances for use with the example MEMCACHE_SERVERS declaration do

memcached -u root -d -m 256 -l 0.0.0.0 -p 11211
memcached -u root -d -m 256 -l 0.0.0.0 -p 11212
