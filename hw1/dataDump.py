import psycopg2
#import xml.etree.ElementTree as ET
#import lxml
#import lmxl.etree
from lxml import etree

def dropTables(conn):
    cur = conn.cursor()
    sql = '''DROP TABLE IF EXISTS Article; 
    DROP TABLE IF EXISTS Inproceedings;
    DROP TABLE IF EXISTS Authorship;'''
    cur.execute(sql)
    conn.commit()


def createTable(conn):
    cur = conn.cursor()
    Article = '''CREATE TABLE Article (
    pubkey TEXT,
    title TEXT,
    journal TEXT,
    year INT);
    '''

    Proceedings = '''CREATE TABLE Inproceedings (
    pubkey TEXT,
    title TEXT,
    booktitle TEXT,
    year INT);
    '''

    Authorship = '''CREATE TABLE Authorship (
    pubkey TEXT NOT NULL,
    author TEXT NOT NULL);
    '''

    cur.execute(Article)
    cur.execute(Proceedings)
    cur.execute(Authorship)
    conn.commit()


# def storeArticle(conn, data):

    
# def storeProceeding(conn, data):

            
        
if __name__ == '__main__':
    conn = psycopg2.connect(dbname='dblp', user='dblpuser', password='123', host='localhost', port='5432')

    dropTables(conn)
    createTable(conn)

    cur = conn.cursor()
    insertion = 0
    for _, element in etree.iterparse('dblp.xml', dtd_validation=True):
        if element.tag == 'article':
            insertion += 1
            pubkey = element.attrib['key']
            res = {}
            author = set([])
            for child in element:
                #print(child.tag)
                if child.tag == 'author' and child.text not in author:
                    #tp = child.text
                    sql = '''INSERT INTO Authorship (pubkey, author) VALUES (%s, %s);'''
                    cur.execute(sql, (pubkey, child.text))
                    author.add(child.text)
                    #conn.commit()
                        
                elif child.tag == 'title':
                    res['title'] = child.text
                elif child.tag == 'year':
                    res['year'] = int(child.text)
                elif child.tag == 'journal':
                    res['journal'] = child.text
                        
            if 'title' not in res.keys():
                res['title'] = None
            if 'journal' not in res.keys():
                res['journal'] = None
            if 'year' not in res.keys():
                res['year'] = None
            sql = '''INSERT INTO Article (pubkey, title, journal, year) VALUES (%s, %s, %s, %s);'''
            cur.execute(sql, (pubkey, res['title'], res['journal'], res['year']))
            element.clear()

        elif element.tag == 'inproceedings':
            insertion += 1
            pubkey = element.attrib['key']
            res = {}
            author = set([])
            for child in element:
                #print(child.tag)
                if child.tag == 'author' and child.text not in author:
                    #tp = child.text
                    sql = '''INSERT INTO Authorship (pubkey, author) VALUES (%s, %s);'''
                    cur.execute(sql, (pubkey, child.text))
                    author.add(child.text)
                    # conn.commit()
        
                elif child.tag == 'title':
                    res['title'] = child.text
                elif child.tag == 'booktitle':
                    res['booktitle'] = child.text
                elif child.tag == 'year':
                    res['year'] = int(child.text)

            if 'title' not in res.keys():
                res['title'] = None
            if 'booktitle' not in res.keys():
                res['booktitle'] = None
            if 'year' not in res.keys():
                res['year'] = None
            
            sql = '''INSERT INTO Inproceedings (pubkey, title, booktitle, year) VALUES (%s, %s, %s, %s);'''
            cur.execute(sql, (pubkey, res['title'], res['booktitle'], res['year']))
            element.clear()
            
        if insertion == 10000:
            insertion = 0
            conn.commit()

        #element.clear()
    conn.commit()


    
# Testing

#for _, element in etree.iterparse('tp.xml', dtd_validation=True):
#    if element.tag == 'article':
#        print('yoyoyo')


# conn = psycopg2.connect(dbname='dblp', user='dblpuser', password='123', host='localhost', port='5432')
# dropTables(conn)
# createTable(conn)
# xmlp = etree.XMLParser(dtd_validation=True)
# tree = etree.parse('tp.xml', parser=xmlp)
# root = tree.getroot()
# for c in root:
#     storeArticle(conn, c)
        
