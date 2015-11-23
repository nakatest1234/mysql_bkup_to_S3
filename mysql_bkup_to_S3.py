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
import datetime
import subprocess
import re
import glob

def main(config):
	# vars
	ymd = time.strftime('%Y%m%d')
	his = time.strftime('%H%M%S')

	# config vars
	server        = config['SERVER']
	scheme        = config['SCHEME']
	prefix        = config['PREFIX']
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

	cmd_base = opt_base + ['-u', config['ID'], '-p'+config['PW'], '-h', server, scheme]

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
		data['basename'] = '{0}.{1}.{2}'.format(basename, data['tag'], config['SUFFIX'])
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

	# exec backup delete
	if (config['BKUP_DAYS']>0):
		exec_backup_delete(config)

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


def exec_backup_delete(config):
	day        = config['BKUP_DAYS']
	target_dir = get_exec_tmp_parent_dir(config)

	if (os.path.exists(target_dir)):
		#cmd = ['find', target_dir, '-type', 'f', '-mtime', '+{0}'.format(day), '-exec', 'rm', '-f', '"{}"', '\\;']
		#print " ".join(cmd)
		#p = subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=False)
		#p.wait()

		#cmd = ['find', target_dir, '-type', 'd', '-empty', '-print0', '|', 'xargs', '--no-run-if-empty', '-0', 'rmdir']
		#print " ".join(cmd)
		#p = subprocess.Popen(cmd, stderr=subprocess.STDOUT, shell=False)
		#p.wait()

		del_date = datetime.datetime.now() - datetime.timedelta(days=day)
		del_time = time.mktime(del_date.timetuple())

		for (root, dirs, files) in os.walk(target_dir):
			for file in files:
				path = os.path.join(root, file)

				if os.path.getmtime(path) < del_time:
					os.remove(path)
					syslog.syslog('remove: {0}'.format(path))

		for (root, dirs, files) in os.walk(target_dir):
			for dir in dirs:
				path = os.path.join(root, dir)
				if len(os.listdir(path))==0:
					os.rmdir(path)
					syslog.syslog('rmdir: {0}'.format(path))

	return True

def get_exec_tmpdir(config, ymd):
	exec_tmpdir = os.path.join(get_exec_tmp_parent_dir(config), ymd)

	# make temporary dir
	syslog.syslog(exec_tmpdir)
	if (os.path.isdir(exec_tmpdir)==False):
		#umask = os.umask(0)
		os.makedirs(exec_tmpdir, 0777)
		#os.umask(umask)

	return exec_tmpdir


def get_exec_tmp_parent_dir(config):
	return os.path.join(config['TMPDIR'], config['SERVER'], config['SCHEME'])


def checkopt(config):
	for k in ['ID', 'PW', 'SCHEME', 'SERVER']:
		if (k not in config or config[k]==False):
			raise Exception(0, k+': not found')

	if ('TMPDIR' not in config or config['TMPDIR']==False or not isinstance(config['TMPDIR'], types.StringType)):
		config['TMPDIR'] = tempfile.gettempdir()

	if ('PREFIX' not in config or config['PREFIX']==False or not isinstance(config['PREFIX'], types.StringType)):
		config['PREFIX'] = 'bkup.mysql'

	if ('SUFFIX' not in config or config['SUFFIX']==False or not isinstance(config['SUFFIX'], types.StringType)):
		config['SUFFIX'] = 'sql'

	if ('GZIP' not in config or config['GZIP']==False or not isinstance(config['GZIP'], types.StringType)):
		config['GZIP'] = 'y'

	if ('BKUP_DAYS' not in config or config['BKUP_DAYS']==False or not isinstance(config['BKUP_DAYS'], types.IntType)):
		config['BKUP_DAYS'] = 0

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
