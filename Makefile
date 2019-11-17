PROJECT = aidb
PROJECT_DESCRIPTION = database tools for production from ailink.io
PROJECT_VERSION = 0.1.9

DEPS = epgsql eredis ailib

dep_epgsql_commit = 4.3.0
dep_eredis_commit = v1.2.0
dep_ailib = git https://github.com/DavidAlphaFox/ailib.git tag-0.3.6

include erlang.mk
