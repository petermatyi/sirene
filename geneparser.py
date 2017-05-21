import sqlite3
import re

conn = sqlite3.connect('geneDB.sqlite')
cur = conn.cursor()

# Setup DB
cur.executescript('''
DROP TABLE IF EXISTS Genes;
DROP TABLE IF EXISTS Organism;

CREATE TABLE Genes (
    gene_id   INTEGER NOT NULL PRIMARY KEY UNIQUE,
    name      TEXT,
    org_id    INTEGER,
    entrez_id INTEGER
);

CREATE TABLE Organism (
    id        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name      TEXT UNIQUE
)
''')

gene_id = 0
entrez_id = 0
name = ''
org_id = 0

fh = open("C:/Users/Matyi/Documents/_matyi/genoid-cellcall/MATLAB/tbox/TRANSFAC/gene.dat")
for line in fh:
    words = line.rstrip().split('  ')
    if words[0] == 'AC':
        gene_id = int(re.findall('G([0-9]+)', words[1])[0])
    elif words[0] == 'SD':
        name = words[1]
    elif words[0] == 'OS':
        org = words[1]
        cur.execute('SELECT id FROM Organism WHERE name = ? ', (org,))
        orgRes = cur.fetchone()
        if orgRes is None:
            cur.execute('''INSERT INTO Organism ( name )
                            VALUES ( ? )''', (org,))
            cur.execute('SELECT id FROM Organism WHERE name = ? ', (org,))
            org_id = cur.fetchone()[0]
        else:
            org_id = orgRes[0]
    elif words[0] == 'DR' and words[1].startswith('ENTREZGENE:'):
        entrez_id = int(re.findall('ENTREZGENE: ([0-9]+)', words[1])[0])
    elif words[0] == '//' and gene_id != 0:
        cur.execute('''INSERT INTO Genes ( gene_id, name, org_id, entrez_id )
                        VALUES ( ?, ?, ?, ? )''', (gene_id, name, org_id, entrez_id))
        entrez_id = 0

fh.close()
conn.commit()
