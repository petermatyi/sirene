#! /usr/bin/env python3

import sqlite3
import re

conn = sqlite3.connect('geneDB.sqlite')
cur = conn.cursor()

# Setup DB
cur.executescript('''
DROP TABLE IF EXISTS Factors;
DROP TABLE IF EXISTS Subunits;
DROP TABLE IF EXISTS Regulations;

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

factor_id = 0
name = ''
org_id = 0
coding_gene = 0
subunits = 0
site_id = 0
quality = 0
reg_gene = 0
reg_org = 0

fh = open("C:/Users/Matyi/Documents/_matyi/genoid-cellcall/MATLAB/tbox/TRANSFAC/factor.dat")
for line in fh:
    words = line.rstrip().split('  ')
    if words[0] == 'AC':
        factor_id = int(re.findall('T([0-9]+)', words[1])[0])
    elif words[0] == 'FA':
        name = words[1]
    elif words[0] == 'OS':
        cur.execute('SELECT id FROM Organism WHERE name = ? ', (words[1],))
        orgRes = cur.fetchone()
        if orgRes is None:
            cur.execute('''INSERT INTO Organism ( name )
                            VALUES ( ? )''', (words[1],))
            cur.execute('SELECT id FROM Organism WHERE name = ? ', (words[1],))
            org_id = cur.fetchone()[0]
        else:
            org_id = orgRes[0]
    elif words[0] == 'GE':
        coding_gene = int(re.findall('G([0-9]+)', words[1])[0])
    elif words[0] == 'ST':
        subunits += 1
        cur.execute('''INSERT INTO Subunits ( factor_id, subunit_id )
                        VALUES ( ?, ? )''', ( factor_id, int(re.findall('T([0-9]+)', words[1])[0])))
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
            orgRes = cur.fetchone()
            if orgRes is None:
                cur.execute('''INSERT INTO Organism ( name )
                                VALUES ( ? )''', (org,))
                cur.execute('SELECT id FROM Organism WHERE name = ? ', (org,))
                reg_org = cur.fetchone()[0]
            else:
                reg_org = orgRes[0]
        else:
            reg_org = 0
        cur.execute('''INSERT INTO Regulations ( site_id, factor_id, quality, reg_gene, org_id )
                                VALUES ( ?, ?, ?, ?, ? )''', (site_id, factor_id, quality, reg_gene, reg_org))
    elif words[0] == '//' and factor_id != 0:
        cur.execute('''INSERT INTO Factors ( factor_id, name, org_id, coding_gene, subunits )
                        VALUES ( ?, ?, ?, ?, ? )''', (factor_id, name, org_id, coding_gene, subunits))
        subunits = 0
        coding_gene = 0

fh.close()
conn.commit()
