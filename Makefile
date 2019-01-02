PROJECT = aidb
PROJECT_DESCRIPTION = database tools for production from ailink.io
PROJECT_VERSION = 0.1.0

DEPS = poolboy epgsql ailib
dep_poolboy_commit = 1.5.2
dep_epgsql_commit = 4.2.1
dep_ailib = git https://github.com/DavidAlphaFox/ailib.git tag-0.1.9
include erlang.mk
