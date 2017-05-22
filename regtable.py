#!/usr/bin/env python3

# - entrez id (genes table) gene id
# - coding gene (factors table) factor id
# - factor id (regulations table) reg gene
# (- subunit id (subunits table) factor id)
# (- factor id (regulations table) reg gene)
# - reg gene (genes table) entrez id
# - filter

import sqlite3

e_ids = []
with open('a.txt') as infile:
    for line in infile:
        e_ids.append(int(line.split()[0]))

conn = sqlite3.connect('geneDB.sqlite')
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
            regs.append((i, e_ids.index(target[0])+1))
        except:
            continue

with open('b.txt', 'w') as outfile:
    for i in sorted(regs):
        print(i[0], i[1])
        outfile.write('%s\t%s\n' % (i[0], i[1]))
