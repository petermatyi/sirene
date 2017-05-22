#!/usr/bin/env python3

import sqlite3
import re
# import urllib.request
# import xml.etree.ElementTree as ET
# import os
# import io
# import gzip
# from ftplib import FTP


def setupdb(fname='geneDB.sqlite'):

    conn = sqlite3.connect(fname)
    cur = conn.cursor()

    cur.executescript('''
    DROP TABLE IF EXISTS Genes;
    DROP TABLE IF EXISTS Organism;
    DROP TABLE IF EXISTS Factors;
    DROP TABLE IF EXISTS Subunits;
    DROP TABLE IF EXISTS Regulations;

    CREATE TABLE Genes (
        gene_id   INTEGER NOT NULL PRIMARY KEY UNIQUE,
        name      TEXT,
        org_id    INTEGER,
        entrez_id INTEGER
    );

    CREATE TABLE Organism (
        id        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name      TEXT UNIQUE
    );
    CREATE TABLE Factors (
    factor_id   INTEGER NOT NULL PRIMARY KEY UNIQUE,
    name        TEXT,
    org_id      INTEGER,
    coding_gene INTEGER,
    subunits    BOOLEAN
    );

    CREATE TABLE Subunits (
    factor_id   INTEGER,
    subunit_id  INTEGER,
    PRIMARY KEY (factor_id, subunit_id)
    );

    CREATE TABLE Regulations (
    id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    site_id     INTEGER,
    factor_id   INTEGER,
    quality     INTEGER,
    reg_gene    INTEGER,
    org_id      INTEGER
    )
    ''')

    conn.commit()


def geneparser(tffile, dbfile):

    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()

    gene_id = 0
    entrez_id = 0
    name = ''
    org_id = 0

    with open(tffile) as fh:
        for line in fh:
            words = line.rstrip().split('  ')
            if words[0] == 'AC':
                gene_id = int(re.findall('G([0-9]+)', words[1])[0])
            elif words[0] == 'SD':
                name = words[1]
            elif words[0] == 'OS':
                org = words[1]
                cur.execute('SELECT id FROM Organism WHERE name = ? ', (org,))
                orgres = cur.fetchone()
                if orgres is None:
                    cur.execute('''INSERT INTO Organism ( name )
                                    VALUES ( ? )''', (org,))
                    cur.execute('SELECT id FROM Organism WHERE name = ? ', (org,))
                    org_id = cur.fetchone()[0]
                else:
                    org_id = orgres[0]
            elif words[0] == 'DR' and words[1].startswith('ENTREZGENE:'):
                entrez_id = int(re.findall('ENTREZGENE: ([0-9]+)', words[1])[0])
            elif words[0] == '//' and gene_id != 0:
                cur.execute('''INSERT INTO Genes ( gene_id, name, org_id, entrez_id )
                                VALUES ( ?, ?, ?, ? )''', (gene_id, name, org_id, entrez_id))
                entrez_id = 0

    conn.commit()


def factorparser(tffile, dbfile):

    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()

    factor_id = 0
    name = ''
    org_id = 0
    coding_gene = 0
    subunits = 0
    # site_id = 0
    # quality = 0
    # reg_gene = 0
    # reg_org = 0

    with open(tffile) as fh:
        for line in fh:
            words = line.rstrip().split('  ')
            if words[0] == 'AC':
                factor_id = int(re.findall('T([0-9]+)', words[1])[0])
            elif words[0] == 'FA':
                name = words[1]
            elif words[0] == 'OS':
                cur.execute('SELECT id FROM Organism WHERE name = ? ', (words[1],))
                orgres = cur.fetchone()
                if orgres is None:
                    cur.execute('''INSERT INTO Organism ( name )
                                    VALUES ( ? )''', (words[1],))
                    cur.execute('SELECT id FROM Organism WHERE name = ? ', (words[1],))
                    org_id = cur.fetchone()[0]
                else:
                    org_id = orgres[0]
            elif words[0] == 'GE':
                coding_gene = int(re.findall('G([0-9]+)', words[1])[0])
            elif words[0] == 'ST':
                subunits += 1
                cur.execute('''INSERT INTO Subunits ( factor_id, subunit_id )
                                VALUES ( ?, ? )''', (factor_id, int(re.findall('T([0-9]+)', words[1])[0])))
            elif words[0] == 'BS':
                reg_words = words[1].rstrip('.').split(';')
                site_id = int(re.findall('R([0-9]+)', reg_words[0])[0])
                quality = int(re.findall('Quality: ([0-9])', reg_words[2])[0])
                if len(reg_words) == 5:
                    reg_gene = int(re.findall(', G([0-9]+)', reg_words[3])[0])
                    org = reg_words[4].lstrip()
                elif len(reg_words) == 4:
                    reg_gene = 0
                    org = reg_words[3].lstrip()
                else:
                    reg_gene = 0
                    org = ''
                if org != '':
                    cur.execute('SELECT id FROM Organism WHERE name = ? ', (org,))
                    orgres = cur.fetchone()
                    if orgres is None:
                        cur.execute('''INSERT INTO Organism ( name )
                                        VALUES ( ? )''', (org,))
                        cur.execute('SELECT id FROM Organism WHERE name = ? ', (org,))
                        reg_org = cur.fetchone()[0]
                    else:
                        reg_org = orgres[0]
                else:
                    reg_org = 0
                cur.execute('''INSERT INTO Regulations ( site_id, factor_id, quality, reg_gene, org_id )
                                        VALUES ( ?, ?, ?, ?, ? )''', (site_id, factor_id, quality, reg_gene, reg_org))
            elif words[0] == '//' and factor_id != 0:
                cur.execute('''INSERT INTO Factors ( factor_id, name, org_id, coding_gene, subunits )
                                VALUES ( ?, ?, ?, ?, ? )''', (factor_id, name, org_id, coding_gene, subunits))
                subunits = 0
                coding_gene = 0

    conn.commit()


def regtable(rfile, dbfile):

    e_ids = []
    with open(rfile) as infile:
        for line in infile:
            e_ids.append(int(line.split()[0]))

    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()

    regs = []
    i = 0

    for e_id in e_ids:
        i += 1
        cur.execute('SELECT gene_id FROM Genes WHERE entrez_id = ? ', (e_id,))
        g_id = cur.fetchone()

        if g_id is None:
            continue

        cur.execute('SELECT factor_id FROM Factors WHERE coding_gene = ? ', (g_id[0],))
        f_id = cur.fetchone()

        if f_id is None:
            continue

        cur.execute('SELECT reg_gene FROM Regulations WHERE factor_id = ? ', (f_id[0],))

        for rg in set(cur.fetchall()):
            cur.execute('SELECT entrez_id FROM Genes WHERE gene_id = ? ', (rg[0],))
            target = cur.fetchone()
            try:
                regs.append((i, e_ids.index(target[0]) + 1))
            except:
                continue

    with open('regulations.txt', 'w') as outfile:
        for i in sorted(regs):
            print(i[0], i[1])
            outfile.write('%s\t%s\n' % (i[0], i[1]))
