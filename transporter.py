#!/bin/python
def log_filter(line):	
	if regex.search(line) is not None:
		my_datetime, asn, city, country, package_name, package_version, package_release, package_kind, repository, ip = "", "", "", "", "", "", "", "", "", ""
		# Get info from each row.
		try:
			my_datetime = line.split("[")[1].split("]")[0].split(":")[0]
			my_datetime = time.strftime("%Y-%m-%d", time.strptime(my_datetime, '%d/%b/%Y'))
		except IndexError:
			return None
		url = line.split('"GET ')[1].split(" ")[0]	
		try:
			repository = repo_pattern.search(line).groups()[0]
			file = url.split("/")[-1]
			try:
				info = package_pattern.match(file).groups()
				package_name = info[0]
				package_version = info[1]
				package_release = info[2]
				package_kind = info[3]
			except AttributeError:
				pass
			ip = ip_pattern.search(line).groups()[1]
			if not ':' in ip:
				try:
					country = gi_country.country_code_by_addr(ip)
				except TypeError:
					pass
				try:
					city    = gi_city.record_by_addr(ip)['city']
				except TypeError:
					pass
				except UnicodeEncodeError:
					pass
				try:
					asn = gi_asn.org_by_addr(ip)
				except TypeError:
					pass
			# id is first!
			result = (("%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\001%s\n") % (-1, my_datetime, asn, city, country, package_name, 0, package_version, package_release, package_kind, repository, ip, url))
		except AttributeError:
			return None
		return result

# def new_name(name):
#   return os.path.join(os.path.dirname(name), "parsed_" + os.path.basename(name))

def get_date_range(array):
	first_date = array[0].split('access_log-')[1]
	last_date  = array[-1].split('access_log-')[1]
	return first_date + "-to-" + last_date
	
def clean_log_names(array):
	tmp = sorted(array)
	if tmp[-1].endswith(datetime.datetime.now().strftime("%d")):
		tmp.pop()
	return tmp

def gzipdir(basedir, archivename):
    assert os.path.isdir(basedir)    
    f_out = gzip.open(archivename, 'wb')
    plogs = glob.glob(os.path.join(basedir,'access_log-*'))
    for plog in plogs:
        f_in = open(plog, 'rb')
        f_out.writelines(f_in)
        f_in.close()
    f_out.close()
                    
import subprocess
import glob
import os
import re
import sys
import datetime
import time
import GeoIP
import shutil
import gzip

# from __future__ import with_statement
# from contextlib import closing
# from zipfile import ZipFile, ZIP_DEFLATED

root_dir = '/var/www/repository.egi.eu'

logs = glob.glob(os.path.join(root_dir,'access_log-*'))

logs = clean_log_names(logs)

if not logs :
	print "No new logs to handle."
	sys.exit(0)

gi_country = GeoIP.open(os.path.dirname(os.path.realpath(__file__)) + '/dbs/GeoIP.dat',GeoIP.GEOIP_MEMORY_CACHE)
gi_city = GeoIP.open(os.path.dirname(os.path.realpath(__file__)) + '/dbs/GeoLiteCity.dat',GeoIP.GEOIP_MEMORY_CACHE)
gi_asn = GeoIP.open(os.path.dirname(os.path.realpath(__file__)) + '/dbs/GeoIPASNum.dat',GeoIP.GEOIP_MEMORY_CACHE)

regex = re.compile('"GET /sw/production/.*(\.rpm|\.deb) ')
package_pattern = re.compile('^([a-zA-Z0-9_\-\+\.\%]*)-([a-zA-Z0-9_\-\+\.]*)-([a-zA-Z0-9_\-\+]*)\..*\.*(rpm|deb)')
ip_pattern = re.compile('(logger|\<someone\>): (.*) - -')
repo_pattern = re.compile('"GET /sw/production/((cas/1)|(sam/1)|(umd/1)|(umd/2)|(umd/candidate/1)|(umd/candidate/2))/')

clean_log_file_path = os.path.dirname(os.path.realpath(__file__)) + '/clean-logs/clean.log'
clean_log = open(clean_log_file_path,'w')

parsed_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'parsed-logs')

date_range = get_date_range(logs)

for log in logs:
	print("Now processing " + log)
	# Filter to a local folder and append them all
	f = open(log,'r')
	for line in f:
		n_line = log_filter(line)
		if n_line:
			clean_log.write(n_line)
	f.close()
	# Rename log file
	print("Moving " + log + " to " + parsed_path)
    # os.rename(log, new_name(log))
	shutil.move(log, parsed_path)

# Close clean file
clean_log.close()

# Check if hadoop exists
print "Check hadoop"
if subprocess.call(['which','hadoop']) == 1:
	print 'No hadoop installed.'
	sys.exit(1)
if subprocess.call(['hadoop','fs','-ls','new_logs']) == 1:
	print 'Not correct hadoop folder.'
	sys.exit(1)

# Upload the file to hadoop
print "Uploading to hadoop"
if subprocess.call(['hadoop', 'fs', '-put', clean_log_file_path, '/user/beeswax/warehouse/processed_logs/clean.log-' + date_range]) == 1:
    print 'Couldn\'t upload clean data to HDFS.'
    sys.exit(1)

# Delete clean log
print "Deleting clean log"
os.remove(clean_log_file_path)

# Compress monthly logs
print "Zipping logs"
archive_name = os.path.join(root_dir, 'archive_access_logs', 'access_log-' + date_range)
# shutil.make_archive(archive_name, 'gztar', parsed_path)
# Python 2.4 compatible
gzipdir(parsed_path, archive_name)

# Deleting parsed logs
print "Deleting parsed logs"
parsed_logs = glob.glob(os.path.join(parsed_path,'access_log-*'))
for parsed_log in parsed_logs:
    os.remove(parsed_log)

sys.exit(0)