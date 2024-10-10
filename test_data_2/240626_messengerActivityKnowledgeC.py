import sqlite3
import io
import sys
sys.path.append(r'C:\Users\admin\PycharmProjects\BiomeIntents\venv\Lib\site-packages')
sys.path.append(r'C:\Users\admin\AppData\Local\Programs\Python\Python39\Lib')
import blackboxprotobuf
import plistlib
from datetime import datetime
from time import strftime, gmtime, timezone
import os

import pkgutil


class Parser:
    def __init__(self):
        pass

    def get_timezone(self):
        return -timezone / 3600

    def get_timestamp(self, key):
        intentverb_dic = {"SendMessage": 49, "StartCall": 28, "StartAudioCall": 21}.get(key)
        return intentverb_dic

    def bs_plistlib_load(self, plist):
        bs = io.BytesIO(plist)
        data = plistlib.load(bs)
        return data

    def create_table(self, sql_cur, sql_query):
        sql_cur.execute(sql_query)

    def insert_val(self, sql_cur, sql_query, insert_tuple, sql_con):
        sql_cur.execute(sql_query, insert_tuple)
        sql_con.commit()

    def proto_to_dict(self, protobuf):
        message, typedef = blackboxprotobuf.decode_message(protobuf)
        return message

    def nested_dict_values(self, protobuf_key, d):
        for k, v in d.items():
            ks = protobuf_key + k + ":"
            if isinstance(v, dict):
                yield from Parser.nested_dict_values(self, ks, v)
            else:
                yield [ks, v]

    def get_only_bytes(self, protobuf):
        result_str = ""
        result_hex = bytearray()
        for listitem in protobuf:
            if isinstance(listitem[1], bytes):
                try:
                    result_str += listitem[0] + " " + listitem[1].decode(encoding="utf-8")
                    result_str += "\n"
                except UnicodeDecodeError:
                    result_hex.extend(listitem[1])
            else:
                continue
        if result_hex == b'':
            no_image_str = bytearray().fromhex("6e 6f 5f 68 65 78 64 75 6d 70")
            result_hex.extend(no_image_str)
        return result_str.removesuffix('\n'), result_hex


class SqlQueries:
    def __init__(self):
        # supported apps
        self.supported_apps_bytes = (b'com.iwilab.KakaoTalk', b'ph.telegra.Telegraph', b'jp.naver.line',
                                     b'com.facebook.Messenger', b'com.burbn.instagram', b'com.towneers.www',
                                     b'com.tencent.xin', b'net.whatsapp.WhatsApp', b'com.toyopagroup.picaboo')
        self.supported_apps_str = ("com.iwilab.KakaoTalk", "jp.naver.line", "ph.telegra.Telegraph",
                                   "com.facebook.Messenger", "com.burbn.instagram", "com.towneers.www",
                                   "com.tencent.xin", "net.whatsapp.WhatsApp", "com.toyopagroup.picaboo")

        # ZSOURCE
        self.selectZSOURCE = "SELECT * FROM ZSOURCE WHERE ZSOURCEID in (\"intents\") and " \
                             "ZBUNDLEID in {};".format(self.supported_apps_str)
        self.createTempZSOURCE = "CREATE TABLE ZSOURCE_COPIED " \
                                 "(Z_PK INT PRIMARY KEY NOT NULL, Z_ENT INTEGER, Z_OPT INTEGER, ZUSERID INTEGER, " \
                                 "ZBUNDLEID VARCHAR, ZDEVICEID VARCHAR, ZGROUPID VARCHAR, ZINTENTID VARCHAR," \
                                 "ZITEMID VARCHAR, ZSOURCEID VARCHAR);"
        self.insertIntoTempZSOURCE = "INSERT INTO " \
                                     "ZSOURCE_COPIED(Z_PK, Z_ENT, Z_OPT, ZUSERID, ZBUNDLEID, ZDEVICEID, " \
                                     "ZGROUPID, ZINTENTID, ZITEMID, ZSOURCEID) VALUES(?,?,?,?,?,?,?,?,?,?);"

        # STRUCT
        self.selectZSTRUCT = "SELECT " \
                             "Z_DKINTENTMETADATAKEY__DIRECTION, Z_DKINTENTMETADATAKEY__INTENTVERB, " \
                             "Z_DKINTENTMETADATAKEY__INTENTCLASS, Z_DKINTENTMETADATAKEY__INTENTTYPE, " \
                             "Z_DKINTENTMETADATAKEY__SERIALIZEDINTERACTION, Z_PK FROM ZSTRUCTUREDMETADATA " \
                             "WHERE Z_DKINTENTMETADATAKEY__SERIALIZEDINTERACTION is not null;"
        self.createTempIntentBLOB = "CREATE TABLE SERIALIZEDINTERACTION_OUTPUT" \
                                    "(Z_PK INT PRIMARY KEY NOT NULL, STARTDATE VARCHAR, ENDDATE VARCHAR, " \
                                    "DURATION REAL, DIRECTION VARCHAR, INTENTVERB VARCHAR, INTENTCLASS VARCHAR, " \
                                    "INTENTTYPE VARCHAR, IDENTIFIER VARCHAR, METADATA VARCHAR, METADATA_HEX BLOB);"
        self.insertIntoTempIntentBLOB = "INSERT INTO SERIALIZEDINTERACTION_OUTPUT(Z_PK, STARTDATE, ENDDATE, " \
                                        "DURATION, DIRECTION, INTENTVERB, INTENTCLASS, INTENTTYPE, IDENTIFIER, " \
                                        "METADATA, METADATA_HEX) VALUES(?,?,?,?,?,?,?,?,?,?,?);"
        # JOIN
        self.createJoinTable = """
                               CREATE TABLE APPINTENTS_WITH_DELETEDRECORDS
                               (ZSOURCE_Z_PK INT PRIMARY KEY NOT NULL,
                               ZUSERID    INTEGER,
                               ZDEVICEID VARCHAR,
                               STARTDATE VARCHAR,
                               ENDDATE VARCHAR,
                               DURATION REAL,
                               ZBUNDLEID VARCHAR,
                               DIRECTION VARCHAR,
                               INTENTVERB VARCHAR,
                               INTENTCLASS VARCHAR,
                               INTENTTYPE VARCHAR,
                               ZGROUPID VARCHAR,
                               ZITEMID VARCHAR,
                               ZSOURCEID VARCHAR,
                               IDENTIFIER VARCHAR,
                               METADATA VARCHAR,
                               METADATA_HEX BLOB);"""
        self.selectJoinTable = """
                               SELECT 
                               ZSOURCE_COPIED.Z_PK,
                               ZSOURCE_COPIED.ZUSERID,
                               ZSOURCE_COPIED.ZDEVICEID,
                               SERIALIZEDINTERACTION_OUTPUT.STARTDATE,
                               SERIALIZEDINTERACTION_OUTPUT.ENDDATE,
                               SERIALIZEDINTERACTION_OUTPUT.DURATION,
                               ZSOURCE_COPIED.ZBUNDLEID,
                               SERIALIZEDINTERACTION_OUTPUT.DIRECTION,
                               SERIALIZEDINTERACTION_OUTPUT.INTENTVERB,
                               SERIALIZEDINTERACTION_OUTPUT.INTENTCLASS,
                               SERIALIZEDINTERACTION_OUTPUT.INTENTTYPE,
                               ZSOURCE_COPIED.ZGROUPID,
                               ZSOURCE_COPIED.ZITEMID,
                               ZSOURCE_COPIED.ZSOURCEID,
                               SERIALIZEDINTERACTION_OUTPUT.IDENTIFIER,
                               SERIALIZEDINTERACTION_OUTPUT.METADATA,
                               SERIALIZEDINTERACTION_OUTPUT.METADATA_HEX
                               FROM ZSOURCE_COPIED
                               LEFT JOIN SERIALIZEDINTERACTION_OUTPUT on 
                               ZSOURCE_COPIED.ZINTENTID = SERIALIZEDINTERACTION_OUTPUT.IDENTIFIER
                               """
        self.insertIntoJoinTable = """ 
                                   INSERT INTO APPINTENTS_WITH_DELETEDRECORDS (ZSOURCE_Z_PK, ZUSERID, ZDEVICEID, 
                                   STARTDATE, ENDDATE, DURATION, ZBUNDLEID, DIRECTION, INTENTVERB, INTENTCLASS, 
                                   INTENTTYPE, ZGROUPID, ZITEMID, ZSOURCEID, IDENTIFIER, METADATA, METADATA_HEX) 
                                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                                   """

        self.selectSharesheet = """
                                SELECT
                                ZOBJECT.ZSTARTDATE AS "STARTDATE", 
                                ZOBJECT.ZENDDATE AS "ENDDATE",
                                ZSOURCE.ZBUNDLEID AS "ZBUNDLEID",
                                ZSOURCE.ZSOURCEID AS "ZSOURCEID",
                                ZSTRUCTUREDMETADATA.Z_DKSHARESHEETFEEDBACKMETADATAKEY__SOURCEBUNDLEID AS "SHARE_SOURCEBUNDLEID", 
                                ZSTRUCTUREDMETADATA.Z_DKSHARESHEETFEEDBACKMETADATAKEY__TARGETBUNDLEID AS "SHARE_TARGETBUNDLEID",
                                ZSTRUCTUREDMETADATA.Z_DKSHARESHEETFEEDBACKMETADATAKEY__TRANSPORTBUNDLEID AS "SHARE_TRANSPORTBUNDLEID",
                                ZSTRUCTUREDMETADATA.Z_DKSHARESHEETFEEDBACKMETADATAKEY__ATTACHMENTS AS "SHARE_ATTACHMENTS",
                                ZSOURCE.Z_PK,
                                ZSTRUCTUREDMETADATA.Z_PK
                                FROM ZOBJECT LEFT JOIN ZSTRUCTUREDMETADATA 
                                ON ZOBJECT.ZSTRUCTUREDMETADATA = ZSTRUCTUREDMETADATA.Z_PK 
                                LEFT JOIN ZSOURCE  ON ZOBJECT.ZSOURCE = ZSOURCE.Z_PK 
                                WHERE Z_DKSHARESHEETFEEDBACKMETADATAKEY__ATTACHMENTS is not null
                                AND ZSTREAMNAME = "/sharesheet/feedback" 
                                AND ZSOURCE.ZBUNDLEID in {};
                                """.format(self.supported_apps_str)
        self.createSharesheet = """
                                CREATE TABLE SHARESHEET_OUTPUT
                                (ZSOURCE_Z_PK INT PRIMARY KEY NOT NULL,
                                ZUSERID    INTEGER,
                                ZDEVICEID VARCHAR,
                                STARTDATE VARCHAR,
                                ENDDATE VARCHAR,
                                DURATION REAL,
                                ZBUNDLEID VARCHAR,
                                DIRECTION VARCHAR,
                                INTENTVERB VARCHAR,
                                INTENTCLASS VARCHAR,
                                INTENTTYPE VARCHAR,
                                ZGROUPID VARCHAR,
                                ZITEMID VARCHAR,
                                ZSOURCEID VARCHAR,
                                IDENTIFIER VARCHAR,
                                METADATA VARCHAR,
                                METADATA_HEX BLOB);
                                """
        self.insertIntoSharesheet = """
                                    INSERT INTO SHARESHEET_OUTPUT
                                    (ZSOURCE_Z_PK, ZUSERID, ZDEVICEID, STARTDATE, 
                                    ENDDATE, DURATION, ZBUNDLEID, DIRECTION,
                                    INTENTVERB, INTENTCLASS, INTENTTYPE, ZGROUPID, ZITEMID, ZSOURCEID, IDENTIFIER, METADATA, 
                                    METADATA_HEX) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                                    """
        self.createUnionTable = """
                                CREATE TABLE UNIONTABLE
                                (ZSOURCE_Z_PK INT PRIMARY KEY NOT NULL,
                                ZUSERID INTEGER,
                                ZDEVICEID VARCHAR,
                                STARTDATE VARCHAR,
                                ENDDATE VARCHAR,
                                DURATION REAL,
                                ZBUNDLEID VARCHAR,
                                DIRECTION VARCHAR,
                                INTENTVERB VARCHAR,
                                INTENTCLASS VARCHAR,
                                INTENTTYPE VARCHAR,
                                ZGROUPID VARCHAR,
                                ZITEMID VARCHAR,
                                ZSOURCEID VARCHAR,
                                IDENTIFIER VARCHAR,
                                METADATA VARCHAR,
                                METADATA_HEX BLOB);"""
        self.selectUnionTable = """
                                SELECT * FROM APPINTENTS_WITH_DELETEDRECORDS
                                UNION
                                SELECT * FROM SHARESHEET_OUTPUT
                                ORDER BY STARTDATE;
                                """
        self.insertIntoUnionTable = """
                                    INSERT INTO UNIONTABLE
                                    (ZSOURCE_Z_PK, ZUSERID, ZDEVICEID, STARTDATE, ENDDATE, DURATION, ZBUNDLEID, 
                                    DIRECTION, INTENTVERB, INTENTCLASS, INTENTTYPE, ZGROUPID, ZITEMID, ZSOURCEID, 
                                    IDENTIFIER, METADATA, METADATA_HEX) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
        self.createFinalTable = """
                                CREATE TABLE MESSENGER_APP_ACTIVITY
                                (ZSOURCE_Z_PK INT PRIMARY KEY NOT NULL,
                                ZSOURCEID VARCHAR,
                                STARTDATE VARCHAR,
                                ENDDATE VARCHAR,
                                DURATION REAL,
                                ZBUNDLEID VARCHAR,
                                USER_ACTIVITY VARCHAR,
                                DIRECTION VARCHAR,
                                METADATA VARCHAR,
                                METADATA_HEX BLOB,
                                ZGROUPID VARCHAR,
                                ZITEMID VARCHAR,
                                IDENTIFIER VARCHAR);"""
        self.selectFinalTable = """
                                SELECT 
                                ZSOURCE_Z_PK, ZSOURCEID, STARTDATE, ENDDATE, DURATION, ZBUNDLEID, INTENTVERB, 
                                DIRECTION, METADATA, METADATA_HEX, ZGROUPID, ZITEMID, IDENTIFIER   
                                FROM UNIONTABLE
                                ORDER BY ZSOURCE_Z_PK;
                                """
        self.insertIntoFinalTable = """
                                    INSERT INTO MESSENGER_APP_ACTIVITY
                                    (ZSOURCE_Z_PK, ZSOURCEID, STARTDATE, ENDDATE, DURATION, ZBUNDLEID, USER_ACTIVITY, 
                                    DIRECTION, METADATA, METADATA_HEX, ZGROUPID, ZITEMID, IDENTIFIER) 
                                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);
                                    """
        
        
def main():
    par1 = Parser()
    query = SqlQueries()

    # usage : messengerActivityKnowledgeC.exe [file_full_path]
    if len(sys.argv) != 2:
        print("\n>> (사용법) messengerActivityKnowledgeC.exe [file_full_path]")
        sys.exit(0)
    print("=== iOS knowledgeC.db parser for messenger apps / based on iOS 16.7.2 ===")
    print("=== supported apps : 카카오톡, 텔레그램, 라인, 페이스북 메신져, 인스타그램, 왓츠앱, 스냅챗 ===")
    inputPath = sys.argv[1]

    con = sqlite3.connect(inputPath)
    cur = con.cursor()

    # output file path
    curTime = datetime.now().strftime('%Y%m%d_%H%M%S_')
    filename = curTime + "Messenger_Activity.db"
    desktoppath = os.path.expanduser("~\\desktop\\")
    filepath = os.path.join(desktoppath, filename)
    con_output_db = sqlite3.connect(filepath)
    cur_output_db = con_output_db.cursor()

    # intents record
    # ZSOURCE
    print("[+] ZSOURCE table : Selecting in progress")

    # Create temp zsource table
    par1.create_table(cur_output_db, query.createTempZSOURCE)

    # Select and insert
    cur.execute(query.selectZSOURCE)
    zsource_rec_tuple = cur.fetchall()
    for i in zsource_rec_tuple:
        par1.insert_val(cur_output_db, query.insertIntoTempZSOURCE, i, con_output_db)

    # ZSTRUCT
    print("[+] ZSTRUCTUREDMETADATA.Z_DKINTENTMETADATAKEY__SERIALIZEDINTERACTION : Parsing in progress")

    # Create temp intent blob table
    par1.create_table(cur_output_db, query.createTempIntentBLOB)

    # Select, parse and insert
    cur.execute(query.selectZSTRUCT)
    serial_blob = cur.fetchall()
    z_pk = 0
    for item in serial_blob:
        z_pk += 1

        # outgoing or incoming
        direction = item[0]
        if direction == 1:
            direction = "1:outgoing"
        elif direction == 2:
            direction = "2:incoming"
        intentverb = item[1]
        intentclass = item[2]
        intenttype = item[3]
        struct_z_pk = item[5]

        # see if the record is related to message or call
        if intentverb not in ("SendMessage", "StartCall", "StartAudioCall"):
            continue

        # outter NSKA parsing
        nsData = par1.bs_plistlib_load(item[4])
        try:
            nsDataPlistlib = nsData['$objects'][1]['NS.data']
        except KeyError:  # NS.data key error 처리
            print("[+] err : ['NS.data'] KeyError in ZSTRUCTUREDMETADATA의 Z_PK : {} // "
                  "Check this row manually".format(struct_z_pk))
            continue

        # inner NSKA parsing
        parsedNsData = par1.bs_plistlib_load(nsDataPlistlib)

        # get protocol buf
        try :
            byteData = parsedNsData['$objects'][4]['bytes']
        except KeyError: # bytes key error 처리
            print("[+] err : ['bytes'] KeyError in ZSTRUCTUREDMETADATA의 Z_PK : {} // "
                  "Check this row manually".format(struct_z_pk))
            continue

        # parse protocol buffer
        protoDict = par1.proto_to_dict(byteData)
        # check app bundle id and parse protobuf
        if protoDict['1']['2'] in query.supported_apps_bytes:
            kstr = ""
            protoValList = list(par1.nested_dict_values(kstr, protoDict))
            protoBytesStr, protoBytesHex = par1.get_only_bytes(protoValList)

            # get uid for timestamp based on intentverb value
            uid = par1.get_timestamp(intentverb)

            # NS.time key error
            try:
                startDate = parsedNsData['$objects'][uid]['NS.time']
                endDate = parsedNsData['$objects'][uid]['NS.time']
                # print(uid-1, parsedNsData['$objects'][uid-1]['NS.startDate'])
                # print(uid-1, parsedNsData['$objects'][uid - 1]['NS.endDate'])
                # convert mac time
                startDate = strftime('%Y-%m-%d %H:%M:%S %z', gmtime(startDate + 978307200 + par1.get_timezone() * 3600))
                endDate = strftime('%Y-%m-%d %H:%M:%S %z', gmtime(endDate + 978307200 + par1.get_timezone() * 3600))
                duration = parsedNsData['$objects'][uid - 1]["NS.duration"]
            except (KeyError, IndexError):  # NS.time key error 처리
                print("[+] err : ['NS.time'] KeyError in ZSTRUCTUREDMETADATA의 Z_PK : {} // "
                      "Check this row manually".format(struct_z_pk))
                startDate = "ZSTRUCTUREDMETADATA.Z_PK : {} parsing error".format(struct_z_pk)

            # Identifier key error
            try:
                identifier = parsedNsData['$objects'][3]
            except KeyError:  # UUID key error 처리
                print("UUID index KeyError in ZSTRUCTUREDMETADATA의 Z_PK : {} // "
                      "Check this row manually".format(struct_z_pk))
        else:
            continue

        # insert
        insert_tuple = (z_pk, startDate, endDate, duration, direction, intentverb, intentclass, intenttype, identifier,
                        protoBytesStr, protoBytesHex)
        par1.insert_val(cur_output_db, query.insertIntoTempIntentBLOB, insert_tuple, con_output_db)

    # join two tables
    par1.create_table(cur_output_db, query.createJoinTable)
    cur_output_db.execute(query.selectJoinTable)
    join_tuple = cur_output_db.fetchall()
    for i in join_tuple:
        par1.insert_val(cur_output_db, query.insertIntoJoinTable, i, con_output_db)

    # sharesheet record
    #print("[+] ZSTRUCTUREDMETADATA.Z_DKSHARESHEETFEEDBACKMETADATAKEY__ATTACHMENTS : Parsing in progress")
    cur.execute(query.selectSharesheet)
    sharesheet_blob = cur.fetchall()
    par1.create_table(cur_output_db, query.createSharesheet)

    for item in sharesheet_blob:
        startDate = item[0]
        endDate = item[1]
        duration = endDate - startDate
        zbundleid = item[2]
        zsourceid = item[3]
        sourcebundleid = item[4]
        targetbundleid = item[5]
        transportbundleid = item[6]
        z_pk = item[8]
        struct_z_pk = item[9]

        # convert mac time
        startDate = strftime('%Y-%m-%d %H:%M:%S %z', gmtime(startDate + 978307200 + par1.get_timezone() * 3600))
        endDate = strftime('%Y-%m-%d %H:%M:%S %z', gmtime(endDate + 978307200 + par1.get_timezone() * 3600))

        # outter NSKA parsing
        outterNSKA = par1.bs_plistlib_load(item[7])

        # inner NSKA parsing
        try:
            nestedNSKA = outterNSKA['$objects'][1]
            parsedNested = par1.bs_plistlib_load(nestedNSKA)
        except KeyError:
            print("[+] err : innerNSKA index KeyError in ZSTRUCTUREDMETADATA의 Z_PK : {} // "
                  "Check this row manually".format(struct_z_pk))

        # file path
        try:
            filepath = parsedNested['$objects'][5]
        except KeyError:
            print("[+] err : filepath index KeyError in ZSTRUCTUREDMETADATA의 Z_PK : {} // "
                  "Check this row manually".format(struct_z_pk))

        # UTI
        try:
            fileMetadata = parsedNested['$objects'][3]
        except KeyError:
            print("[+] err : fileMetadata index KeyError in ZSTRUCTUREDMETADATA의 Z_PK : {} // "
                  "Check this row manually".format(struct_z_pk))

        # combine findings
        metadataAll = "targetbundleid : " + str(targetbundleid) + '\n' + "transportbundleid : " + str(
            transportbundleid) + \
                      '\n' + "filepath : " + str(filepath) + '\n' + "UTI : " + str(fileMetadata)
        insert_tuple = (z_pk, None, None, startDate, endDate, duration, zbundleid, None, None, None, None,
                        None, None, zsourceid, None, metadataAll, "no_hexdump")

        par1.insert_val(cur_output_db, query.insertIntoSharesheet, insert_tuple, con_output_db)

    # union

    par1.create_table(cur_output_db, query.createUnionTable)
    cur_output_db.execute(query.selectUnionTable)
    union_result = cur_output_db.fetchall()

    for i in union_result:
        par1.insert_val(cur_output_db, query.insertIntoUnionTable, i, con_output_db)

    # final output
    par1.create_table(cur_output_db, query.createFinalTable)
    cur_output_db.execute(query.selectFinalTable)
    final_result = cur_output_db.fetchall()
    for i in final_result:
        i = list(i)
        if i[1] == "intents" and i[6] == "SendMessage":
            i[6] = "Chat"
        elif i[1] == "intents" and i[6] in ("StartCall", "StartAudioCall"):
            i[6] = "Call"
        elif i[1] == "sharesheet":
            i[6] = "SaveFile"
        i = tuple(i)
        par1.insert_val(cur_output_db, query.insertIntoFinalTable, i, con_output_db)

    sqlQueryDrop = "DROP TABLE APPINTENTS_WITH_DELETEDRECORDS;"
    cur_output_db.execute(sqlQueryDrop)
    sqlQueryDrop = "DROP TABLE SHARESHEET_OUTPUT;"
    cur_output_db.execute(sqlQueryDrop)
    sqlQueryDrop = "DROP TABLE SERIALIZEDINTERACTION_OUTPUT;"
    cur_output_db.execute(sqlQueryDrop)
    sqlQueryDrop = "DROP TABLE ZSOURCE_COPIED;"
    cur_output_db.execute(sqlQueryDrop)
    sqlQueryDrop = "DROP TABLE UNIONTABLE;"
    cur_output_db.execute(sqlQueryDrop)

    con.close()
    con_output_db.close()

    print("[+] The output DB file is created on your desktop directory.")
    print("[+] Complete")


if __name__ == '__main__':
    main()


