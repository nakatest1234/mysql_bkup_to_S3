#! /usr/bin/env python
# -*- coding: utf-8 -*- 


#from pprint import pprint
import os
import sys
import yaml
import types
import tempfile
import syslog
import time
import subprocess
import re

def main(config):
	# vars
	ymd = time.strftime('%Y%m%d')
	his = time.strftime('%H%M%S')

	# config vars
	server        = config['SERVER']
	scheme        = config['SCHEME']
	prefix        = config['PREFIX']
	id            = config['ID']
	pw            = config['PW']
	opt_base      = config['OPT_BASE']
	ignore_tables = config['IGNORE_TABLES']
	split_tables  = config['SPLIT_TABLES']

	# vars
	list_ignore = ignore_tables
	exec_tmpdir = get_exec_tmpdir(config, ymd)
	dumplist    = []

	# make ignore table list
	if (len(split_tables)>0):
		for tables in split_tables.itervalues():
			list_ignore.extend(tables)

	# prefix
	if (config['PREFIX']):
		prefix = prefix + '.'

	basename = '{0}{1}.{2}.{3}-{4}'.format(prefix, server, scheme, ymd, his)
	bkupfile_base = os.path.join(exec_tmpdir, basename)

	cmd_base   = opt_base + ['-u', id, '-p'+pw, '-h', server, scheme]

	# create
	dumplist.append({
		'tag': 'create',
		'cmd': ['-d'],
	})

	# data
	dumplist.append({
		'tag': 'data',
		'cmd': map(lambda x, s: '--ignore-table={0}.{1}'.format(s,x), list_ignore, [scheme] * len(list_ignore)) + ['-t', '-c'],
	})

	# data(split tables)
	for table in split_tables:
		dumplist.append({
			'tag': 'data.' + table,
			'cmd': ['-t', '-c'] + split_tables[table]
		})

	# make dumplist hash
	for data in dumplist:
		data['basename'] = '{0}.{1}.sql'.format(basename, data['tag'])
		data['path']     = os.path.join(exec_tmpdir, data['basename'])
		data['cmd']      = [config['CMD_DUMP'], '--result_file={0}'.format(data['path'])] + cmd_base + data['cmd']
		data['s3_src']   = data['path'] if (config['GZIP'].lower()!='y') else data['path'] + '.gz'

	# exec mysqldump
	exec_mysqldump(dumplist)

	# exec gzip
	if (config['GZIP'].lower()=='y'):
		exec_gzip(config['CMD_GZIP'], dumplist)

	# exec S3 upload
	if (config['S3_DIR']):
		exec_s3_upload(config['S3_DIR'], dumplist)

	return True


def exec_mysqldump(dumplist):
	for data in dumplist:
		syslog.syslog('mysqldump: START {0}'.format(data['basename']))

		p = subprocess.Popen(data['cmd'], stderr=subprocess.STDOUT, shell=False)
		p.wait()

		syslog.syslog('mysqldump: END {0}'.format(data['basename']))

	return True


def exec_gzip(gzip, dumplist):
	for data in dumplist:
		syslog.syslog('gzip: START {0}'.format(data['basename']))

		p = subprocess.Popen([gzip, data['path']], stderr=subprocess.STDOUT, shell=False)
		p.wait()

		syslog.syslog('gzip: END {0}'.format(data['basename']))

	return True


def exec_s3_upload(s3_path, dumplist):
	#s3_path = re.sub(r'^s3://|/$', '', s3_path, re.IGNORECASE)
	s3_path = re.sub(r'^s3://|/$', '', s3_path)

	for data in dumplist:
		syslog.syslog('s3: START {0}'.format(data['s3_src']))
		cmd = ['aws', 's3', 'cp', '--quiet', data['s3_src'], 's3://{0}/'.format(s3_path)]

		p = subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=False)
		p.wait()

		syslog.syslog('s3: END {0}'.format(data['basename']))

	return True


def get_exec_tmpdir(config, ymd):
	exec_tmpdir = os.path.join(config['TMPDIR'], config['SERVER'], config['SCHEME'], ymd)

	# make temporary dir
	syslog.syslog(exec_tmpdir)
	if (os.path.isdir(exec_tmpdir)==False):
		#umask = os.umask(0)
		os.makedirs(exec_tmpdir, 0777)
		#os.umask(umask)

	return exec_tmpdir


def checkopt(config):
	for k in ['ID', 'PW', 'SCHEME', 'SERVER']:
		if (k not in config or config[k]==False):
			raise Exception(0, k+': not found')

	if ('TMPDIR' not in config or config['TMPDIR']==False or not isinstance(config['TMPDIR'], types.StringType)):
		config['TMPDIR'] = tempfile.gettempdir()

	if ('PREFIX' not in config or config['PREFIX']==False or not isinstance(config['PREFIX'], types.StringType)):
		config['PREFIX'] = 'bkup.mysql'

	if ('GZIP' not in config or config['GZIP']==False or not isinstance(config['GZIP'], types.StringType)):
		config['GZIP'] = 'y'

	if ('CMD_DUMP' not in config or config['CMD_DUMP']==False or not isinstance(config['CMD_DUMP'], types.StringType)):
		config['CMD_DUMP'] = 'mysqldump'

	if ('CMD_GZIP' not in config or config['CMD_GZIP']==False or not isinstance(config['CMD_GZIP'], types.StringType)):
		config['CMD_GZIP'] = 'gzip'

	if ('S3_DIR' not in config or config['S3_DIR']==False or not isinstance(config['S3_DIR'], types.StringType)):
		config['S3_DIR'] = ''

	if ('OPT_BASE' not in config or not isinstance(config['OPT_BASE'], types.ListType)):
		config['OPT_BASE'] = ['--quick', '--add-drop-table', '--add-locks', '--extended-insert', '--order-by-primary', '--single-transaction', '--skip-triggers']

	if ('IGNORE_TABLES' not in config or not isinstance(config['IGNORE_TABLES'], types.ListType)):
		config['IGNORE_TABLES'] = []

	if ('SPLIT_TABLES' not in config or not isinstance(config['SPLIT_TABLES'], types.DictType)):
		config['SPLIT_TABLES'] = {}

	return config


if __name__ == '__main__':

	args = sys.argv
	argc = len(args)

	if (argc == 2):
		if (os.path.isfile(args[1])):
			try:
				config = checkopt(yaml.load(file(args[1])))
				main(config)
			except NameError, e:
				print >> sys.stderr, "NameError:", e.args[0]
			except Exception, e:
				print >> sys.stderr, "Exception:", e.args[0], e.args[1]
			except:
				print >> sys.stderr, "Unexpected error:", sys.exc_info()[0]
		else:
			print u'{0} not exists.'.format(args[1])
			quit()
	else:
		print u'Usage: python {0} <config yaml>'.format(args[0])
		quit()
