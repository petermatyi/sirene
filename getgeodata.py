#! /usr/bin/env python2.7

import urllib
import xml.etree.ElementTree as ET
import os
import StringIO
import gzip
from ftplib import FTP

while True:
    s = raw_input('Enter GEO dataset ID:')
    try:
        geoID = int(s)
    except:
        print 'Enter a number.'
        continue

    # Search summary
    data = urllib.urlopen('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&id=%s' % geoID).read()
    tree = ET.fromstring(data)

    # No record
    if tree.find('.//ERROR') is not None:
        print 'No ID in GEO matched your expression:', geoID
        continue
    title = tree.find(".//*[@Name='title']").text

    # Retired record
    if title.startswith('Retired:'):
        print 'Retired dataset, choose another.'
        continue
    else:
        print 'Dataset:', title
        directory = './GDS%s' % geoID
        if not os.path.exists(directory):
            os.makedirs(directory)

        gpl = int(tree.find(".//*[@Name='GPL']").text)
        ftpLink = tree.find(".//*[@Name='FTPLink']").text

        # Downloading GPL annot.gz
        print 'Downloading GPL...',
        ftp = FTP('ftp.ncbi.nlm.nih.gov')
        ftp.login()
        if gpl < 1000:
            ftp.cwd('geo/platforms/GPLnnn/GPL%s/annot/' % gpl)
        else:
            ftp.cwd('geo/platforms/GPL%snnn/GPL%s/annot/' % (gpl/1000, gpl))
        compFile = StringIO.StringIO()
        ftp.retrbinary('RETR GPL%s.annot.gz' % gpl, compFile.write)
        print 'OK.'

        # Extracting .gz
        print 'Extracting GPL...',
        compFile.seek(0)
        decompFile = gzip.GzipFile(fileobj=compFile)
        with open(directory + '/GPL%s.annot' % gpl, 'wb') as gplFile:
            gplFile.write(decompFile.read())
        decompFile.close()
        compFile.close()
        print 'OK.'

        # Downloading DNA array data
        print 'Downloading dataset...',
        ftp.cwd(ftpLink.replace('ftp://ftp.ncbi.nlm.nih.gov/', '../../../../../')+'soft/')
        ftp.pwd()
        compFile = StringIO.StringIO()
        ftp.retrbinary('RETR GDS%s_full.soft.gz' % geoID, compFile.write)
        print 'OK.'

        # Extracting .gz
        print 'Extracting dataset...',
        compFile.seek(0)
        decompFile = gzip.GzipFile(fileobj=compFile)
        with open(directory + '/GDS%s_full.soft' % geoID, 'wb') as geoFile:
            geoFile.write(decompFile.read())
        decompFile.close()
        compFile.close()
        print 'OK.'

        ftp.quit()
        break
