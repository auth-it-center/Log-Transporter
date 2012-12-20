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
		
import subprocess
import glob
import os
import re
import sys
import datetime
import time
import GeoIP

logs = glob.glob('/var/www/repository.egi.eu/tasos_logs/clean_with_get_and_deb_rpm.log')

gi_country = GeoIP.open(os.path.dirname(os.path.realpath(__file__)) + "/dbs/GeoIP.dat",GeoIP.GEOIP_MEMORY_CACHE)
gi_city = GeoIP.open(os.path.dirname(os.path.realpath(__file__)) + "/dbs/GeoLiteCity.dat",GeoIP.GEOIP_MEMORY_CACHE)
gi_asn = GeoIP.open(os.path.dirname(os.path.realpath(__file__)) + "/dbs/GeoIPASNum.dat",GeoIP.GEOIP_MEMORY_CACHE)

regex = re.compile('"GET /sw/production/.*(\.rpm|\.deb) ')
package_pattern = re.compile('^([a-zA-Z0-9_\-\+\.\%]*)-([a-zA-Z0-9_\-\+\.]*)-([a-zA-Z0-9_\-\+]*)\..*\.*(rpm|deb)')
ip_pattern = re.compile('(logger|\<someone\>): (.*) - -')
repo_pattern = re.compile('"GET /sw/production/((cas/1)|(sam/1)|(umd/1)|(umd/2)|(umd/candidate/1)|(umd/candidate/2))/')

clean_log_path = os.path.dirname(os.path.realpath(__file__)) + "/clean-logs/clean.log"
clean_log = open(clean_log_path,'w')

for log in logs:
	print("now processing " + log)
	# Filter to a local folder and append them all
	f = open(log,'r')
	for line in f:
		n_line = log_filter(line)
		if n_line:
			clean_log.write(n_line)
	f.close()
		
# Close clean file
clean_log.close()

# Check if hadoop exists
print "check hadoop"
if subprocess.call(['which','hadoop']) == 1:
	print 'No hadoop installed.'
	sys.exit(1)
if subprocess.call(['hadoop','fs','-ls','new_logs']) == 1:
	print 'Not correct hadoop folder.'
	sys.exit(1)

#upload the file to hadoop
print "uploading to hadoop"
now = str(datetime.datetime.now()).replace(" ",".").replace(":",".")
subprocess.call(['hadoop', 'fs', '-put', os.path.dirname(os.path.realpath(__file__)) + "/clean-logs/clean.log", '/user/beeswax/warehouse/processed_logs/clean.log' + now])

#delete
print "deleting clean logs"
os.remove(clean_log_path)

sys.exit(0)