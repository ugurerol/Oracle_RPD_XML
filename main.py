import os
import xml.etree.ElementTree as ET
from datetime import datetime
import json
import pandas as pd
from collections import defaultdict 

print(f"************************************************\nSorgu Baslama Saati: {datetime.now()}")

###########################################################################
####################  DEGISTIRILECEK_ALAN   ############################### 
## download edilecek yer := 
download_path = r"C:\Users\ugure\Downloads"                         

generate_json_output = True        ## json olusturulsun mu.
generate_txt_output = True         ## txt olusturulsun mu.
generate_excel_output = True       ## excel olusturulsun mu.
generate_oracle_db_output = False  ## oracle'da tablo olusturulsun mu.

## Sirket adi := 
sirket = ''

## RPD XML'in path'i :=
base_path = r"C:\Users\ugure\Desktop\oracle\bi\server\base"

## oracle db baglantisi := 
ORACLE_DB_USER = "" 
ORACLE_DB_PASSWORD = "" 
ORACLE_DB_DSN = "" 
ORACLE_SCHEMA = "" 
ORACLE_TABLE_NAME = "" 
####################  DEGISTIRILECEK_ALAN   ###############################
###########################################################################

# ---------------------- Tanimlar basl. ----------------------
base_path_db = fr"{base_path}\Database"
base_path_sch = fr"{base_path}\Schema"
base_path_cp = fr"{base_path}\ConnectionPool"
base_path_pt = fr"{base_path}\PhysicalTable"
base_path_lt = fr"{base_path}\LogicalTable"
base_path_bm = fr"{base_path}\BusinessModel"
base_path_lts = fr"{base_path}\LogicalTableSource"
base_path_lcj = fr"{base_path}\LogicalComplexJoin"
base_path_prs = fr"{base_path}\PresentationTable"
base_path_pc = fr"{base_path}\PresentationCatalog"

ns = {"obis": "http://www.oracle.com/obis/repository"}

sysdate = datetime.now().strftime("%Y%m%d_%H%M%S")

def parse_xml(file_path):
    if not os.path.exists(file_path):
        return None
    try:
        return ET.parse(file_path).getroot()
    except Exception as e:
        print(f"Hata: {file_path} okunamadi -> {e}")
        return None

def get_physicaltable_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    pt = root.find("obis:PhysicalTable", ns)
    return pt if pt is not None else (root if root.tag.endswith("PhysicalTable") else None)

def get_schema_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    sc = root.find("obis:Schema", ns)
    return sc if sc is not None else (root if root.tag.endswith("Schema") else None)

def get_database_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    db = root.find("obis:Database", ns)
    return db if db is not None else (root if root.tag.endswith("Database") else None)

def get_connectionpool_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    cp = root.find("obis:ConnectionPool", ns)
    return cp if cp is not None else (root if root.tag.endswith("ConnectionPool") else None)

def get_logicaltable_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    lt = root.find("obis:LogicalTable", ns)
    return lt if lt is not None else (root if root.tag.endswith("LogicalTable") else None)

def get_businessmodel_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    bm = root.find("obis:BusinessModel", ns)
    return bm if bm is not None else (root if root.tag.endswith("BusinessModel") else None)

def get_logicaltablesource_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    lts = root.find("obis:LogicalTableSource", ns)
    return lts if lts is not None else (root if root.tag.endswith("LogicalTableSource") else None)

def get_presentationtable_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    prs = root.find("obis:PresentationTable", ns)
    return prs if prs is not None else (root if root.tag.endswith("PresentationTable") else None)

def get_presentationcatalog_node(file_path):
    root = parse_xml(file_path)
    if root is None:
        return None
    pc = root.find("obis:PresentationCatalog", ns)
    return pc if pc is not None else (root if root.tag.endswith("PresentationCatalog") else None)
# ---------------------- Tanimlar bitis. ----------------------



# ---------------------- PhysicalTable basl. ----------------------
# --- PhysicalColumn temp tablo ---
pt_temp_tables_columns = {}
for file_name in os.listdir(base_path_pt):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_pt, file_name)
        pt_node = get_physicaltable_node(file_path)
        if pt_node is None:
            continue
        for col in pt_node.findall("obis:PhysicalColumn", ns):
            col_mdsid = col.attrib.get("mdsid", "")
            col_name = col.attrib.get("name", "")
            if col_mdsid:
                pt_temp_tables_columns[f"{file_name}#{col_mdsid}"] = col_name

# --- PhysicalTable parse ---
def parse_physical_table(file_path):
    pt_node = get_physicaltable_node(file_path)
    if pt_node is None:
        return None

    # --- Columns ---
    columns_list = []
    for col in pt_node.findall("obis:PhysicalColumn", ns):
        parts = [col.attrib.get("dataType","")]
        if col.get("precision"):
            parts.append(col.get("precision"))
        else:
            parts.append("")
        parts.append(col.attrib.get("nullable",""))
        parts.append(col.attrib.get("specialType",""))
        # col_str = f'{col.attrib.get("name","")} ({",".join([f"{p}" for p in parts])})'
        col_str = f"'{col.attrib.get('name','')}' ({','.join([f'{p}' for p in parts])})"
        columns_list.append(col_str) 
    columns_str = "; ".join(columns_list)

    # --- Foreign Keys ---
    fk_str_list = []
    for fk in pt_node.findall("obis:PhysicalForeignKey", ns):
        fk_name = fk.attrib.get("name", "")
        ref_cols = []
        for rc in fk.findall(".//obis:RefColumn", ns):
            ref = rc.attrib.get("columnRef", "")
            if ref:
                xml_file = os.path.basename(file_path)
                key = f"{xml_file}#{ref.split('#')[-1]}"
                col_name = pt_temp_tables_columns.get(key, ref.split('#')[-1])
                ref_cols.append(col_name)

        # --- Counterpart table ekle ---
        counterpart_table = ""
        counterpart_ref = fk.attrib.get("counterPartKeyRef", "")
        if counterpart_ref and "/" in counterpart_ref: 
            xml_path = os.path.join(base_path_pt, os.path.basename(counterpart_ref.split("#")[0]))
            if os.path.exists(xml_path):
                cp_node = get_physicaltable_node(xml_path)
                if cp_node is not None: 
                    counterpart_table = cp_node.attrib.get("name", "")
            else: 
                counterpart_table = os.path.basename(counterpart_ref.split("#")[0])


        if ref_cols:
            if counterpart_table:
                fk_str_list.append(f"{fk_name}: {','.join(ref_cols)} ({counterpart_table})")
            else:
                fk_str_list.append(f"{fk_name}: {','.join(ref_cols)}")
    fk_str_all = "; ".join(fk_str_list)

    # --- Primary Keys ---
    pk_str_list = []
    for pk in pt_node.findall("obis:PhysicalKey", ns):
        pk_name = pk.attrib.get("name", "")
        ref_cols = []
        for rc in pk.findall(".//obis:RefColumn", ns):
            ref = rc.attrib.get("columnRef", "")
            if ref:
                xml_file = os.path.basename(file_path)
                key = f"{xml_file}#{ref.split('#')[-1]}"
                col_name = pt_temp_tables_columns.get(key, ref.split('#')[-1])
                ref_cols.append(col_name)
        if ref_cols:
            pk_str_list.append(f"{pk_name}: {','.join(ref_cols)}")
    pk_str_all = "; ".join(pk_str_list)

    # --- Schema ref ---
    pt_container_ref = pt_node.attrib.get("containerRef","")
    pt_schema_mdsid = pt_container_ref.split("#")[-1] if pt_container_ref else ""

    # --- Source PT name ---
    pt_source_name = ""
    pt_source_ref = pt_node.attrib.get("sourceTableRef","")
    if pt_source_ref and "#" in pt_source_ref:
        pt_source_mdsid = pt_source_ref.split("#")[-1]
        for fn in os.listdir(base_path_pt):
            if fn.lower().endswith(".xml"):
                fp = os.path.join(base_path_pt, fn)
                src_pt_node = get_physicaltable_node(fp)
                if src_pt_node is not None and src_pt_node.attrib.get("mdsid") == pt_source_mdsid:
                    pt_source_name = src_pt_node.attrib.get("name","")
                    break

    pt_temp_dict = {
        "pt_xml_name": os.path.basename(file_path),
        "pt_mdsid": pt_node.attrib.get("mdsid",""),
        "pt_name": pt_node.attrib.get("name",""),
        "pt_type": pt_node.attrib.get("type",""),
        "pt_cacheExpiry": pt_node.attrib.get("cacheExpiry",""),
        "pt_hints": pt_node.attrib.get("hints",""),
        "pt_maxConn": pt_node.attrib.get("maxConn",""),
        "pt_description": pt_node.findtext("obis:Description","",namespaces=ns),
        "pt_dbMapItem": pt_node.find("obis:DBMapItem", ns).attrib.get("name","") if pt_node.find("obis:DBMapItem", ns) is not None else "",
        "pt_columns": columns_str, 
        "pt_physical_keys": pk_str_all,
        "pt_foreign_keys": fk_str_all,
        "pt_schema_mdsid": pt_schema_mdsid,
        "pt_source_name": pt_source_name
    }
    return pt_temp_dict

# --- PT listesi ---
pt_temp_list = []
for file_name in os.listdir(base_path_pt):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_pt, file_name)
        pt_temp_dict = parse_physical_table(file_path)
        if pt_temp_dict:
            pt_temp_list.append(pt_temp_dict)

# --- Schema listesi ---
sc_temp_list = []
for file_name in os.listdir(base_path_sch):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_sch, file_name)
        schema_node = get_schema_node(file_path)
        if schema_node is None:
            continue
        sc_temp_dict = {
            "sc_mdsid": schema_node.attrib.get("mdsid",""),
            "sc_name": schema_node.attrib.get("name",""),
            "sc_database_mdsid": schema_node.attrib.get("containerRef","").split("#")[-1] if schema_node.attrib.get("containerRef") else ""
        }
        sc_temp_list.append(sc_temp_dict)

# --- Database listesi ---
db_temp_list = []
for file_name in os.listdir(base_path_db):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_db, file_name)
        db_node = get_database_node(file_path)
        if db_node is None:
            continue

        db_connectionpool_mdsid_list = []
        for ref_cp in db_node.findall(".//obis:RefConnectionPools/obis:RefConnectionPool", ns):
            connectionpool_ref = ref_cp.attrib.get("connectionPoolRef","")
            if connectionpool_ref:
                db_connectionpool_mdsid_list.append(connectionpool_ref.split("#")[-1])

        db_temp_dict = {
            "db_mdsid": db_node.attrib.get("mdsid",""),
            "db_name": db_node.attrib.get("name",""),
            "db_type": db_node.attrib.get("type",""),
            "db_dbName": db_node.attrib.get("dbName",""),
            "db_dbTypeId": db_node.attrib.get("dbTypeId",""), 
            "db_connectionpool_mdsid": db_connectionpool_mdsid_list[0] if db_connectionpool_mdsid_list else ""
        }
        db_temp_list.append(db_temp_dict)

# --- ConnectionPool listesi ---
cp_temp_list = []
for file_name in os.listdir(base_path_cp):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_cp, file_name)
        cp_node = get_connectionpool_node(file_path)
        if cp_node is None:
            continue

        cp_temp_dict = {
            "connectionpool_mdsid": cp_node.attrib.get("mdsid",""),
            "connectionpool_name": cp_node.attrib.get("name",""),
            "connectionpool_user": cp_node.attrib.get("user",""),
            "connectionpool_timeout": cp_node.attrib.get("timeout",""),
            "connectionpool_maxConnDiff": cp_node.attrib.get("maxConnDiff",""),
            "connectionpool_maxConn": cp_node.attrib.get("maxConn",""),
            "connectionpool_dataSource": cp_node.attrib.get("dataSource",""),
            "connectionpool_type": cp_node.attrib.get("type",""),
            "connectionpool_reqQualifedTableName": cp_node.attrib.get("reqQualifedTableName",""),
            "connectionpool_isSharedLogin": cp_node.attrib.get("isSharedLogin",""),
            "connectionpool_isConcurrentQueriesInConnection": cp_node.attrib.get("isConcurrentQueriesInConnection",""),
            "connectionpool_isCloseAfterEveryRequest": cp_node.attrib.get("isCloseAfterEveryRequest",""),
            "connectionpool_outputType": cp_node.attrib.get("outputType",""),
            "connectionpool_ignoreFirstLine": cp_node.attrib.get("ignoreFirstLine",""),
            "connectionpool_bulkInsertBufferSize": cp_node.attrib.get("bulkInsertBufferSize",""),
            "connectionpool_tempTablePrefix": cp_node.attrib.get("tempTablePrefix",""),
            "connectionpool_transactionBoundary": cp_node.attrib.get("transactionBoundary",""),
            "connectionpool_xmlaUseSession": cp_node.attrib.get("xmlaUseSession",""),
            "connectionpool_isSiebelJDBSecured": cp_node.attrib.get("isSiebelJDBSecured","") 
        }
        cp_temp_list.append(cp_temp_dict)

# --- PT → Schema → Database → ConnectionPool join ---
pt_list = []
for pt in pt_temp_list:
    pt_copy = pt.copy()
    
    schema_info = next((sc for sc in sc_temp_list if sc["sc_mdsid"] == pt.get("pt_schema_mdsid")), None)
    if schema_info:
        pt_copy["pt_schema_name"] = schema_info["sc_name"]
        db_info = next((db for db in db_temp_list if db["db_mdsid"] == schema_info.get("sc_database_mdsid")), None)
        if db_info:
            pt_copy["pt_database_mdsid"] = db_info["db_mdsid"]
            pt_copy["pt_database_name"] = db_info["db_name"]
            pt_copy["pt_database_type"] = db_info["db_type"]
            pt_copy["pt_database_dbName"] = db_info["db_dbName"]
            pt_copy["pt_database_dbTypeId"] = db_info["db_dbTypeId"]

            cp_info = next((cp for cp in cp_temp_list if cp["connectionpool_mdsid"] == db_info["db_connectionpool_mdsid"]), None)
            if cp_info:
                for key, value in cp_info.items():
                    pt_copy[f"pt_{key}"] = value
            else:
                for key in [k for k in cp_temp_list[0].keys() if k != "connectionpool_mdsid"]:
                    pt_copy[f"pt_{key}"] = ""
        else:
            # DB yoksa tum DB ve ConnectionPool alanlari bos
            for key in ["pt_database_name","pt_database_mdsid","pt_database_type","pt_database_dbName","pt_database_dbTypeId"]:
                pt_copy[key] = ""
            for key in [k for k in cp_temp_list[0].keys() if k != "connectionpool_mdsid"]:
                pt_copy[f"pt_{key}"] = ""
    else:
        # Schema yoksa tum DB ve ConnectionPool alanlari bos
        for key in ["pt_schema_name","pt_database_name","pt_database_mdsid","pt_database_type","pt_database_dbName","pt_database_dbTypeId"]:
            pt_copy[key] = ""
        for key in [k for k in cp_temp_list[0].keys() if k != "connectionpool_mdsid"]:
            pt_copy[f"pt_{key}"] = ""
    
    pt_list.append(pt_copy) 
# ---------------------- PhysicalTable bitis. ----------------------



# ---------------------- LogicalTable basl. ----------------------
# --- LogicalTable parse ---
def parse_logical_table(file_path):
    lt_node = get_logicaltable_node(file_path)
    if lt_node is None:
        return None
    
    # subjectAreaRef -> BusinessModel MDSID
    businessmodel_ref = lt_node.attrib.get("subjectAreaRef", "")
    businessmodel_mdsid = ""
    if businessmodel_ref and "#" in businessmodel_ref:
        businessmodel_mdsid = businessmodel_ref.split("#")[-1]
    
    # RefTableSources -> RefLogicalTableSource -> logicalTableSourceRef -> LogicalTableSource MDSID
    logical_table_source_mdsid = ""
    ref_table_sources = lt_node.find("obis:RefTableSources", ns)
    if ref_table_sources is not None:
        ref_logical_source = ref_table_sources.find("obis:RefLogicalTableSource", ns)
        if ref_logical_source is not None:
            logical_table_source_ref = ref_logical_source.attrib.get("logicalTableSourceRef", "")
            if "#" in logical_table_source_ref:
                logical_table_source_mdsid = logical_table_source_ref.split("#")[-1]

    # --- LogicalColumns bilgileri ---
    lt_columns_all_list = []
    lt_aggregate_columns_list = []
    lt_columns_list = []

    for col in lt_node.findall("obis:LogicalColumn", ns):
        col_name = "'" + col.attrib.get("name", "") + "'"
        if col_name:
            lt_columns_list.append(col_name)  # sadece isimler

        attr_expr = ""  # AttributeDefn
        meas_expr = ""  # MeasureDefn

        # Attribute kolon ise
        attr_node = col.find("obis:AttributeDefn/obis:ExprTextDesc", ns)
        if attr_node is not None and attr_node.text:
            attr_expr = attr_node.text.strip().replace('"', '\'')

        # Measure kolon ise
        meas_node = col.find("obis:MeasureDefn/obis:AggrRule/obis:Expr/obis:ExprTextDesc", ns)
        if meas_node is not None and meas_node.text:
            meas_expr = meas_node.text.strip().replace('"', '\'')

        # lt_columns_all icin
        col_parts = []
        if col_name:
            col_parts.append(col_name)
        if attr_expr:
            col_parts.append(f'({attr_expr})')
        if meas_expr:
            col_parts.append(f'({meas_expr})')
        if col_parts:
            lt_columns_all_list.append(" ".join(col_parts))

        aggregate_func = ""
        if meas_expr and not meas_expr.endswith('%1') and '(' in meas_expr:
            aggregate_func = meas_expr.split('(')[0].upper()
        if aggregate_func:
            lt_aggregate_columns_list.append(f"{col_name} ({aggregate_func})")

    lt_columns_all_str = "; ".join(lt_columns_all_list)
    lt_aggregate_columns_str = "; ".join(lt_aggregate_columns_list)
    lt_columns_str = "; ".join(lt_columns_list) 

    lt_temp_dict = {
        "lt_xml_name": os.path.basename(file_path),
        "lt_mdsid": lt_node.attrib.get("mdsid", ""),
        "lt_name": lt_node.attrib.get("name", ""),
        "lt_columns": lt_columns_str,
        "lt_columns_all": lt_columns_all_str,
        "lt_aggregate_columns": lt_aggregate_columns_str,
        "lt_businessmodel_mdsid": businessmodel_mdsid,
        "lt_logicalTableSource_mdsid": logical_table_source_mdsid
    }
    return lt_temp_dict

# --- LT listesi ---
lt_temp_list = []
for file_name in os.listdir(base_path_lt):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_lt, file_name)
        lt_temp_dict = parse_logical_table(file_path)
        if lt_temp_dict:
            lt_temp_list.append(lt_temp_dict)

# --- BusinessModel listesi ---
bm_temp_list = [] 
for file_name in os.listdir(base_path_bm):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_bm, file_name)
        bm_node = get_businessmodel_node(file_path)
        if bm_node is None:
            continue
        bm_temp_dict = {
            "bm_mdsid": bm_node.attrib.get("mdsid",""),
            "bm_name": bm_node.attrib.get("name",""),
            "bm_isClassicStar": bm_node.attrib.get("isClassicStar",""),
            "bm_isAvailable": bm_node.attrib.get("isAvailable","")
        }
        bm_temp_list.append(bm_temp_dict)

# --- LT_Type ---
lt_multiplicity_map = {}
for lcj_file in os.listdir(base_path_lcj):
    if not lcj_file.endswith(".xml"):
        continue
    lcj_root = parse_xml(os.path.join(base_path_lcj, lcj_file))
    if lcj_root is None:
        continue
    lt1_ref = lcj_root.attrib.get("logicalTable1Ref")
    mult1 = lcj_root.attrib.get("multiplicity1")
    if lt1_ref and mult1:
        lt1_mdsid = lt1_ref.split("#")[-1]
        lt_multiplicity_map.setdefault(lt1_mdsid, []).append(mult1.strip())
    lt2_ref = lcj_root.attrib.get("logicalTable2Ref")
    mult2 = lcj_root.attrib.get("multiplicity2")
    if lt2_ref and mult2:
        lt2_mdsid = lt2_ref.split("#")[-1]
        lt_multiplicity_map.setdefault(lt2_mdsid, []).append(mult2.strip())

lt_type_map = {}
for lt_mdsid, mult_list in lt_multiplicity_map.items():
    if all(m in ("0..n", "1..n") for m in mult_list):
        lt_type_map[lt_mdsid] = "Fact"
    else:
        lt_type_map[lt_mdsid] = "Dimension"


# --- LT listesine BM bilgilerini ekle ve LT_TYPE ata --- 
for lt in lt_temp_list:
    # BM bilgileri
    bm_info = next((bm for bm in bm_temp_list if bm["bm_mdsid"] == lt.get("lt_businessmodel_mdsid")), None)
    if bm_info: 
        lt["lt_businessmodel_mdsid"] = bm_info.get("bm_mdsid","")
        lt["lt_businessmodel_name"] = bm_info.get("bm_name","")
        lt["lt_businessmodel_isClassicStar"] = bm_info.get("bm_isClassicStar","")
        lt["lt_businessmodel_isAvailable"] = bm_info.get("bm_isAvailable","")
    else:
        lt["lt_businessmodel_mdsid"] = ""
        lt["lt_businessmodel_name"] = ""
        lt["lt_businessmodel_isClassicStar"] = ""
        lt["lt_businessmodel_isAvailable"] = ""
    
    # LT_TYPE bilgisi
    lt_mdsid = lt.get("lt_mdsid")
    lt["lt_type"] = lt_type_map.get(lt_mdsid, "N/A")


lt_list = lt_temp_list

# --- JSON ciktisi TEST icin---
generate_lt_json_output = False
if generate_lt_json_output:
    json_file = fr"{download_path}\{sirket}_LT_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(lt_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyasi olusturuldu: {json_file}")
# ---------------------- LogicalTable bitis. ----------------------



# ---------------------- LogicalTableSource basl. ----------------------
# --- LogicalTableSource parse ---
def parse_logical_table_source(file_path):
    lts_node = get_logicaltablesource_node(file_path)
    if lts_node is None:
        return None

    # LogicalTable MDSID’sini logicalTableRef’ten cek
    logicaltable_ref = lts_node.attrib.get("logicalTableRef", "")
    logicaltable_mdsid = ""
    if "#" in logicaltable_ref:
        logicaltable_mdsid = logicaltable_ref.split("#")[-1]

    # --- Kolonlari parse et ve string olustur ---
    col_str_list_all = []
    col_list_logical = []
    for cm in lts_node.findall("obis:ColumnMapping", ns):
        logical_expr = cm.find("obis:LogicalColumnExpr", ns)
        expr = cm.find("obis:Expr", ns)

        logical_col_name = ""
        expr_full_text = ""

        if logical_expr is not None:
            desc_node = logical_expr.find("obis:ExprTextDesc", ns)
            if desc_node is not None and desc_node.text:
                logical_col_name = desc_node.text.strip().split(".")[-1].replace('"', '\'').strip()

        if expr is not None:
            desc_node = expr.find("obis:ExprTextDesc", ns)
            if desc_node is not None and desc_node.text:
                expr_full_text = desc_node.text.strip().replace('"', '\'')

        if logical_col_name:
            col_list_logical.append(logical_col_name)
        if logical_col_name and expr_full_text:
            col_str_list_all.append(f"{logical_col_name} ({expr_full_text})")

    # --- WhereClause parse ---
    lts_whereclause = ""
    where_clause_node = lts_node.find("obis:WhereClause", ns)
    if where_clause_node is not None:
        desc_node = where_clause_node.find("obis:ExprTextDesc", ns)
        if desc_node is not None and desc_node.text:
            # Satırlari tek satira indiriyoruz
            lts_whereclause = desc_node.text.strip().replace('"', '\'').replace("\n", " ")

    lts_temp_dict = {
        "lts_xml_name": os.path.basename(file_path),
        "lts_mdsid": lts_node.attrib.get("mdsid", ""),
        "lts_name": lts_node.attrib.get("name", ""),
        "lts_logicaltable_mdsid": logicaltable_mdsid,
        "lts_columns_all": "; ".join(col_str_list_all),
        "lts_columns": "; ".join(col_list_logical),
        "lts_whereclause": lts_whereclause
    } 

    return lts_temp_dict

lts_list = []
for file_name in os.listdir(base_path_lts):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_lts, file_name)
        lts_temp_dict = parse_logical_table_source(file_path)
        if lts_temp_dict:
            lts_list.append(lts_temp_dict)

# --- JSON ciktisi TEST icin---
generate_lts_json_output = False
if generate_lts_json_output:
    json_file = fr"{download_path}\{sirket}_LTS_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(lts_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyasi olusturuldu: {json_file}")
# ---------------------- LogicalTableSource bitis. ----------------------



# ---------------------- PresentationTable basl. ----------------------
# --- PresentationTable parse ---
def parse_presentation_table(file_path):
    prs_node = get_presentationtable_node(file_path)
    if prs_node is None:
        return None
    
    container_ref = prs_node.attrib.get("containerRef", "")
    presentationcatalog_mdsid = container_ref.split("#")[-1] if "#" in container_ref else ""

    # PresentationColumn'lar
    col_nodes = prs_node.findall("obis:PresentationColumn", ns)
    col_names = []
    for col in col_nodes:
        col_name = col.attrib.get("name", "")
        # Alias node'u bul
        alias_node = col.find("obis:Alias", ns)
        alias_name = alias_node.attrib.get("name", "") if alias_node is not None else ""
        if alias_name:  # alias varsa
            col_names.append(f"'{col_name}' ('{alias_name}')")
        else:
            col_names.append(f"'{col_name}'")

    col_names_str = "; ".join(col_names)

    # PresentationTable alias (onceki gibi)
    alias_node_table = prs_node.find("obis:Alias", ns)
    alias_name_table = alias_node_table.attrib.get("name", "") if alias_node_table is not None else ""

    prs_temp_dict = {
        "prs_xml_name": os.path.basename(file_path),
        "prs_mdsid": prs_node.attrib.get("mdsid", ""),
        "prs_name": prs_node.attrib.get("name", ""),
        "prs_alias": alias_name_table,
        "prs_hasdispname": prs_node.attrib.get("hasDispName", ""),
        "prs_hasdispdescription": prs_node.attrib.get("hasDispDescription", ""),
        "prs_columns": col_names_str,
        "prs_presentationcatalog_mdsid": presentationcatalog_mdsid
    }
    return prs_temp_dict


# --- PresentationTable listesi ---
prs_temp_list = []
for file_name in os.listdir(base_path_prs):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_prs, file_name)
        prs_temp_dict = parse_presentation_table(file_path)
        if prs_temp_dict:
            prs_temp_list.append(prs_temp_dict)

# --- PresentationCatalog listesi ---
pc_temp_list = []
for file_name in os.listdir(base_path_pc):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_pc, file_name)
        pc_node = get_presentationcatalog_node(file_path)
        if pc_node is None:
            continue
        
        # BusinessModel mdsid'si
        subject_area_ref = pc_node.attrib.get("subjectAreaRef", "")
        bm_mdsid = subject_area_ref.split("#")[-1] if "#" in subject_area_ref else ""
        
        pc_temp_dict = {
            "pc_mdsid": pc_node.attrib.get("mdsid", ""),
            "pc_name": pc_node.attrib.get("name", ""),
            "pc_hasdispname": pc_node.attrib.get("hasDispName", ""),
            "pc_hasdispdescription": pc_node.attrib.get("hasDispDescription", ""),
            "pc_isexportkeys": pc_node.attrib.get("isExportKeys", ""),
            "pc_isautoaggr": pc_node.attrib.get("isAutoAggr", ""),
            "pc_businessmodel_mdsid": bm_mdsid
        }
        pc_temp_list.append(pc_temp_dict)

# --- prs_temp_list'e PresentationCatalog bilgileri ekleme ---
pc_dict_map = {pc["pc_mdsid"]: pc for pc in pc_temp_list}
for prs in prs_temp_list:
    pc_entry = pc_dict_map.get(prs["prs_presentationcatalog_mdsid"])
    if pc_entry:
        prs["prs_presentationcatalog_name"] = pc_entry.get("pc_name", "")
        prs["prs_presentationcatalog_hasdispname"] = pc_entry.get("pc_hasdispname", "")
        prs["prs_presentationcatalog_hasdispdescription"] = pc_entry.get("pc_hasdispdescription", "")
        prs["prs_presentationcatalog_isexportkeys"] = pc_entry.get("pc_isexportkeys", "")
        prs["prs_presentationcatalog_isautoaggr"] = pc_entry.get("pc_isautoaggr", "")
    else:
        prs["prs_presentationcatalog_name"] = ""
        prs["prs_presentationcatalog_hasdispname"] = ""
        prs["prs_presentationcatalog_hasdispdescription"] = ""
        prs["prs_presentationcatalog_isexportkeys"] = ""
        prs["prs_presentationcatalog_isautoaggr"] = ""

# --- prs_temp_list'e BusinessModel name ekleme ---
bm_dict_map = {bm["bm_mdsid"]: bm["bm_name"] for bm in bm_temp_list}
for prs in prs_temp_list:
    pc_entry = pc_dict_map.get(prs["prs_presentationcatalog_mdsid"])
    if pc_entry:
        bm_name = bm_dict_map.get(pc_entry["pc_businessmodel_mdsid"], "")
        prs["prs_businessmodel_name"] = bm_name
    else:
        prs["prs_businessmodel_name"] = ""

prs_list = prs_temp_list

# --- JSON ciktisi TEST icin ---
generate_prs_json_output = False
if generate_prs_json_output:
    json_file = fr"{download_path}\{sirket}_PRS_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(prs_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyasi olusturuldu: {json_file}")
# ---------------------- PresentationTable bitis. ----------------------



# ---------------------- LT + LTS join basl. ----------------------
lt_lts_list = []

for lt in lt_list:
    lt_copy = lt.copy()
    
    # lt_logicalTableSource_mdsid ile lts_list'ten eslesen satiri bul
    lts_info = next((lts for lts in lts_list if lts["lts_mdsid"] == lt.get("lt_logicalTableSource_mdsid")), None)
    
    if lts_info:
        lt_copy["lts_mdsid"] = lts_info.get("lts_mdsid", "")
        lt_copy["lts_name"] = lts_info.get("lts_name", "")
        lt_copy["lts_columns_all"] = lts_info.get("lts_columns_all", "")
        lt_copy["lts_columns"] = lts_info.get("lts_columns", "")
        lt_copy["lts_whereclause"] = lts_info.get("lts_whereclause", "")
    else:
        # Eslesme yoksa bos birak
        lt_copy["lts_mdsid"] = ""
        lt_copy["lts_name"] = ""
        lt_copy["lts_columns_all"] = ""
        lt_copy["lts_columns"] = ""
        lt_copy["lts_whereclause"] = ""
    
    lt_lts_list.append(lt_copy)

# --- JSON ciktisi TEST icin---
generate_lt_lts_json_output = False
if generate_lt_lts_json_output:
    json_file = fr"{download_path}\{sirket}_LT_LTS_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(lt_lts_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyasi olusturuldu: {json_file}")
# ---------------------- LT + LTS join bitis. ----------------------



# ---------------------- LogicalTable + PhysicalTable mapping basl.----------------------
def extract_physical_tables_from_logical_table(lt_node):
    physical_files = set()
    for lc in lt_node.findall(".//obis:LogicalColumn", ns):
        for ref in lc.findall(".//obis:RefObject", ns):
            obj_ref = ref.attrib.get("objectRef", "")
            if "PhysicalTable" in obj_ref:
                physical_file = os.path.basename(obj_ref.split("#")[0])
                physical_files.add(physical_file)
    return list(physical_files)

bridge_lt_pt_list = [] 

for file_name in os.listdir(base_path_lt):
    if not file_name.lower().endswith(".xml"):
        continue
    lt_path = os.path.join(base_path_lt, file_name)
    lt_node = get_logicaltable_node(lt_path)
    if lt_node is None:
        continue
    logical_table_mdsid = lt_node.attrib.get("mdsid")
    logical_table_name = lt_node.attrib.get("name")
    physical_tables = extract_physical_tables_from_logical_table(lt_node)

    for pt in physical_tables:
        bridge_lt_pt_list.append({
            "lt_mdsid": logical_table_mdsid,
            "lt_name": logical_table_name,
            "lt_physicaltable_xml_name": pt
        })

# --- JSON ciktisi TEST icin---
generate_lt_pt_mapping_json_output = False
if generate_lt_pt_mapping_json_output:
    lt_json_file = fr"{download_path}\{sirket}_LT_PT_MAPPING_RPD_XML_{sysdate}.json"
    with open(lt_json_file, "w", encoding="utf-8") as f:
        json.dump(bridge_lt_pt_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyasi olusturuldu: {lt_json_file}")
# ---------------------- LogicalTable + PhysicalTable mapping bitis.----------------------



# ---------------------- PRS + LT join basl. -----------------------
def extract_prs_logicaltables(file_path):
    """
    Bir PresentationTable XML dosyasindan:
    - PresentationTable mdsid
    - Tum farkli LogicalTable xml adlarini dondurur.
    """
    prs_node = get_presentationtable_node(file_path)
    if prs_node is None:
        return []

    prs_mdsid = prs_node.attrib.get("mdsid", "")
    col_nodes = prs_node.findall("obis:PresentationColumn", ns)

    logical_table_set = set()
    for col in col_nodes:
        logical_column_ref = col.attrib.get("logicalColumnRef", "")
        if logical_column_ref and ".xml" in logical_column_ref:
            xml_name = logical_column_ref.split("/")[-1].split("#")[0]
            logical_table_set.add(xml_name)

    # her LogicalTable icin ayri dict
    result = [{"prs_mdsid": prs_mdsid, "prs_logicaltable_xml": xml_name} for xml_name in logical_table_set]
    return result


# --- tum PresentationTable dosyalari icin ---
prs_lt_join_list = []
for file_name in os.listdir(base_path_prs):
    if file_name.lower().endswith(".xml"):
        file_path = os.path.join(base_path_prs, file_name)
        data_list = extract_prs_logicaltables(file_path)
        prs_lt_join_list.extend(data_list)

# --- JSON ciktisi TEST icin---
generate_prs_lt_join_json_output = False
if generate_prs_lt_join_json_output:
    json_file = fr"{download_path}\{sirket}_PRS_LT_JOIN_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(prs_lt_join_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyasi olusturuldu: {json_file}")
# ---------------------- PRS + LT join bitis. ----------------------



# ---------------------- LT uniue + PRS join basl. -----------------------
def join_lt_unique_prs_lists(prs_lt_join_list, prs_list):
    """
    prs_lt_join_list ile prs_list'i prs_mdsid üzerinden joinler.
    Her eşleşme için tüm kolonlar eklenir.
    """
    # prs_list'i prs_mdsid bazinda grupla
    prs_dict = defaultdict(list)
    for item in prs_list:
        mdsid = item.get("prs_mdsid")
        if mdsid:
            prs_dict[mdsid].append(item)

    # join islemi
    joined_list = []

    for lt_item in prs_lt_join_list:
        mdsid = lt_item.get("prs_mdsid")
        if mdsid in prs_dict:
            for prs_item in prs_dict[mdsid]:
                # lt_item ve prs_item'i birlestir
                new_item = {**lt_item}  # shallow copy
                for k, v in prs_item.items():
                    if k in new_item:
                        new_item[f"{k}_prs"] = v  # cakisirsa _prs ekle
                    else:
                        new_item[k] = v
                joined_list.append(new_item)
        else:
            # eslesme yoksa sadece lt_item ekle
            joined_list.append({**lt_item})

    return joined_list 

# Join
joined_prs_list = join_lt_unique_prs_lists(prs_lt_join_list, prs_list)

# --- JSON ciktisi TEST icin---
generate_lt_unique_prs_1_json_output = False
if generate_lt_unique_prs_1_json_output:
    json_file = fr"{download_path}\{sirket}_LT_PRS_JOIN_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(joined_prs_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyası oluşturuldu: {json_file}")


def make_prs_mdsid_and_columns_list(joined_prs_list, columns_to_number=None):
    """
    joined_prs_list'ten prs_logicaltable_xml bazında uniq liste oluşturur.
    Aynı logicaltable için tüm eşleşen değerleri numaralandırarak birleştirir.
    
    columns_to_number: Numara verilecek kolonlar listesi (örn. ['prs_mdsid', 'prs_name', 'prs_columns'])
    """
    if columns_to_number is None:
        columns_to_number = ['prs_xml_name','prs_mdsid','prs_name','prs_alias','prs_hasdispname','prs_hasdispdescription','prs_columns','prs_presentationcatalog_mdsid','prs_presentationcatalog_name','prs_presentationcatalog_hasdispname','prs_presentationcatalog_hasdispdescription','prs_presentationcatalog_isexportkeys','prs_presentationcatalog_isautoaggr','prs_businessmodel_name']

    grouped = defaultdict(list)
    for item in joined_prs_list:
        xml = item.get("prs_logicaltable_xml")
        if xml:
            grouped[xml].append(item)

    uniq_list = []
    for xml, items in grouped.items():
        new_item = {"prs_logicaltable_xml": xml}
        for col in columns_to_number: 
            numbered_values = "; ".join(f"{i+1}. '{item.get(col,'')}'" for i, item in enumerate(items))
            new_item[col] = numbered_values
        uniq_list.append(new_item)

    return uniq_list

columns_to_number = ['prs_xml_name','prs_mdsid','prs_name','prs_alias','prs_hasdispname','prs_hasdispdescription','prs_columns','prs_presentationcatalog_mdsid','prs_presentationcatalog_name','prs_presentationcatalog_hasdispname','prs_presentationcatalog_hasdispdescription','prs_presentationcatalog_isexportkeys','prs_presentationcatalog_isautoaggr','prs_businessmodel_name']
lt_uniq_joined_prs_list = make_prs_mdsid_and_columns_list(joined_prs_list, columns_to_number)

# --- JSON ciktisi TEST icin---
generate_lt_unique_prs_2_json_output = False
if generate_lt_unique_prs_2_json_output:
    json_file = fr"{download_path}\{sirket}_LT_UNIQUE_PRS_JOIN_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(lt_uniq_joined_prs_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyası oluşturuldu: {json_file}")
# ---------------------- LT uniue + PRS join bitis. ----------------------



# ---------------------- PT + LT + LTS + PRS join basl. ----------------------
all_list = []

# PRS tarafini dict’e al (lookup hizli olsun)
prs_dict = {p["prs_logicaltable_xml"]: p for p in lt_uniq_joined_prs_list}

for pt in pt_list:
    # pt_xml_name ile bridge_lt_pt_list eslesmesi
    matching_bridge = [b for b in bridge_lt_pt_list if b["lt_physicaltable_xml_name"] == pt["pt_xml_name"]]
    
    if matching_bridge:
        for bridge in matching_bridge:
            combined = pt.copy()
            
            # LT + LTS eslesmesi
            lt_lts_info = next((l for l in lt_lts_list if l["lt_mdsid"] == bridge["lt_mdsid"]), None)
            
            if lt_lts_info:
                combined.update(lt_lts_info)  # lt_* ve lts_* alanlari direkt geliyor
            else:
                # LT/LTS alanlarini bos birak
                combined.update({
                    "lt_xml_name": "",
                    "lt_mdsid": "",
                    "lt_name": "",
                    "lt_columns": "",
                    "lt_columns_all": "",
                    "lt_aggregate_columns": "",
                    "lt_businessmodel_mdsid": "",
                    "lt_logicalTableSource_mdsid": "",
                    "lt_businessmodel_name": "",
                    "lt_businessmodel_isClassicStar": "",
                    "lt_businessmodel_isAvailable": "",
                    "lt_type": "",
                    "lts_mdsid": "",
                    "lts_name": "",
                    "lts_columns_all": "",
                    "lts_columns": "",
                    "lts_whereclause": ""
                })
            
            # --- PRS join (lt_xml_name → prs_logicaltable_xml) ---
            lt_xml_name = combined.get("lt_xml_name")
            prs_info = prs_dict.get(lt_xml_name)
            if prs_info:
                for k, v in prs_info.items():
                    if k != "prs_logicaltable_xml":
                        combined[k] = v
            else:
                # PRS alanlarini bos birak
                for k in lt_uniq_joined_prs_list[0].keys():
                    if k != "prs_logicaltable_xml":
                        combined[k] = ""

            all_list.append(combined)
    else:
        combined = pt.copy()
        combined.update({
            "lt_xml_name": "",
            "lt_mdsid": "",
            "lt_name": "",
            "lt_columns": "",
            "lt_columns_all": "",
            "lt_aggregate_columns": "",
            "lt_businessmodel_mdsid": "",
            "lt_logicalTableSource_mdsid": "",
            "lt_businessmodel_name": "",
            "lt_businessmodel_isClassicStar": "",
            "lt_businessmodel_isAvailable": "",
            "lt_type": "",
            "lts_mdsid": "",
            "lts_name": "",
            "lts_columns_all": "",
            "lts_columns": "",
            "lts_whereclause": ""
        })
        # PRS alanlarini da bos birak
        for k in lt_uniq_joined_prs_list[0].keys():
            if k != "prs_logicaltable_xml":
                combined[k] = ""
        all_list.append(combined)
# ---------------------- PT + LT + LTS + PRS join bitis. ----------------------



# ---------------------- Yukleme basl. ----------------------
if generate_json_output:
    json_file = fr"{download_path}\{sirket}_RPD_XML_{sysdate}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(all_list, f, ensure_ascii=False, indent=4)
    print(f"* JSON dosyasi olusturuldu: {json_file}")


if generate_txt_output: 
    txt_file_path = fr"{download_path}\{sirket}_RPD_XML_{sysdate}.txt"
    with open(txt_file_path, "w", encoding="utf-8") as f:
        for item in all_list:
            for key, value in item.items():
                f.write(f"{key}: {value}\n")
            f.write("******************\n")
    print(f"* TXT dosyasi olusturuldu: {txt_file_path}")


if generate_excel_output:
    excel_file_path = fr"{download_path}\{sirket}_RPD_XML_{sysdate}.xlsx" 
    df = pd.DataFrame(all_list) 
    df.to_excel(excel_file_path, index=False)
    print(f"* EXCEL dosyasi olusturuldu: {excel_file_path}")


if generate_oracle_db_output:
    import math
    import oracledb

    def get_max_lengths(data):
        """ JSON icindeki her key icin maksimum string uzunlugunu dondurur. """
        max_lengths = {}
        for record in data:
            for key, value in record.items():
                if value is None:
                    continue
                length = len(str(value))
                max_lengths[key] = max(max_lengths.get(key, 0), length)
        return max_lengths

    def ensure_table_exists_dynamic(cursor, schema, table_name, user, all_list):
        """ 4000 Sinirini asan kolonları otomatik parcalayan tablo olusturur. Eger tablo varsa, DROP TABLE PURGE yapar. """
        check_sql = f""" SELECT COUNT(*) FROM ALL_TABLES WHERE 1=1 AND OWNER = :owner AND TABLE_NAME = :tname """
        cursor.execute(check_sql, {"owner": user.upper(), "tname": table_name.upper()})
        exists = cursor.fetchone()[0]

        if exists:
            drop_sql = f"DROP TABLE {schema}.{table_name} PURGE" 
            cursor.execute(drop_sql)
            cursor.connection.commit()

        max_lengths = get_max_lengths(all_list)

        columns_sql = ["ETL_DATE DATE DEFAULT SYSDATE"]
        seen_columns = set()

        for key, max_len in max_lengths.items():
            col_name = key.upper()
            if max_len <= 4000:
                size = max(100, min(max_len + 50, 4000))
                if col_name not in seen_columns:
                    columns_sql.append(f"\"{col_name}\" VARCHAR2({size})")
                    seen_columns.add(col_name)
            else:
                parts = math.ceil(max_len / 4000)
                for i in range(1, parts + 1):
                    part_col = f"{col_name}_{str(i).zfill(2)}"
                    if part_col not in seen_columns:
                        columns_sql.append(f"\"{part_col}\" VARCHAR2(4000)")
                        seen_columns.add(part_col)

        create_sql = f""" CREATE TABLE {schema}.{table_name} ({', '.join(columns_sql)}) """
        cursor.execute(create_sql) 

    conn = oracledb.connect(user=ORACLE_DB_USER, password=ORACLE_DB_PASSWORD, dsn=ORACLE_DB_DSN)
    cursor = conn.cursor()

    ensure_table_exists_dynamic(cursor, ORACLE_SCHEMA, ORACLE_TABLE_NAME, ORACLE_DB_USER, all_list)

    max_lengths = get_max_lengths(all_list)
    
    insert_cols = []
    insert_placeholders = []

    for key, max_len in max_lengths.items():
        key_upper = key.upper()
        key_lower = key.lower()

        if max_len <= 4000:
            insert_cols.append(f"\"{key_upper}\"")
            insert_placeholders.append(f":{key_lower}")
        else:
            parts = math.ceil(max_len / 4000)
            for i in range(1, parts + 1):
                insert_cols.append(f"\"{key_upper}_{str(i).zfill(2)}\"")
                insert_placeholders.append(f":{key_lower}_{str(i).zfill(2)}")

    insert_sql = f""" INSERT INTO {ORACLE_SCHEMA}.{ORACLE_TABLE_NAME} ({', '.join(insert_cols)}) VALUES ({', '.join(insert_placeholders)}) """

    # --- 4000 uzeri kolonlar icin parcalama ---
    processed_data = []
    for record in all_list:
        new_row = {}
        for key, max_len in max_lengths.items():
            val = str(record.get(key, "")) if record.get(key) is not None else ""
            if max_len <= 4000:
                new_row[key.lower()] = val
            else:
                parts = math.ceil(len(val) / 4000)
                for i in range(1, parts + 1):
                    start = (i - 1) * 4000
                    end = i * 4000
                    new_row[f"{key.lower()}_{str(i).zfill(2)}"] = val[start:end]
        processed_data.append(new_row)

    cursor.executemany(insert_sql, processed_data)
    conn.commit()

    print(f"* Oracle DB ({ORACLE_DB_DSN}) {len(processed_data)} kayit basariyla eklendi.")

    cursor.close()
    conn.close()
# ---------------------- Yukleme bitis. ----------------------



print(f"Sorgu Bitis Saati: {datetime.now()} \n************************************************")
