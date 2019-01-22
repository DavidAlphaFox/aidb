PROJECT = aidb
PROJECT_DESCRIPTION = database tools for production from ailink.io
PROJECT_VERSION = 0.1.2

DEPS = poolboy epgsql eredis ailib
dep_poolboy_commit = 1.5.2
dep_epgsql_commit = 4.2.1
dep_eredis_commit =  v1.2.0
dep_ailib = git https://github.com/DavidAlphaFox/ailib.git tag-0.2.7
include erlang.mk
