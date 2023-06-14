from flask import Flask, request
from flask_cors import CORS, cross_origin
import os
import numpy as np
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.llms import OpenAI
from langchain.chains.question_answering import load_qa_chain
from langchain.vectorstores import utils
from langchain.document_loaders.csv_loader import CSVLoader
from langchain.docstore.document import Document
import json
from textract import create_textract, analyze_invoice, get_summary, get_table, type_invoice, get_object
from csv_embed import embeding
from utils import parse_file_path, convert_epoch, remove_first_space
from schema import schema_generator, find_relationship
from dotenv import load_dotenv
import pymongo
from bson.objectid import ObjectId
from sshtunnel import SSHTunnelForwarder
from datetime import datetime

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

load_dotenv()
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION')

WASABI_ACCESS_KEY = os.getenv('WASABI_ACCESS_KEY')
WASABI_SECRET_KEY = os.getenv('WASABI_SECRET_KEY')

OPEN_AI_KEY = os.getenv('OPEN_AI_KEY')
DB_URL = os.getenv("DB_URL")

# myclient = pymongo.MongoClient(DB_URL)
# admin_db = myclient["rovuk_admin"]

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASS = os.getenv('MONGO_PASS')

server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username=MONGO_USER,
    ssh_password=MONGO_PASS,
    remote_bind_address=('127.0.0.1', 27017)
)

server.start()

myclient = pymongo.MongoClient('127.0.0.1', server.local_bind_port) # server.local_bind_port is assigned local port
admin_db = myclient[MONGO_DB]

admin_collections = {
    "COMPANY": "tenants",
    "API_COUNT": "api_count"
}

company_collections = {
    "INVOICE": "ap_invoices", 
    "PURCHASE_ORDER": "ap_pos", 
    "PACKING_SLIP": "ap_packagingslips", 
    "RECEIVING_SLIP": "ap_receivingslips",
    "INVOICE_LIST": "ap_document_processes",
    "QUOTE": "ap_quotes",
    "OTHER": "ap_otherdocuments",
    "VENDOR": "invoice_vendors",
    "API_COUNT": "api_count"
}

list_to_convertINT = ["tax", "sub_total", "quote_total", "invoice_total_amount", "amount_due", "po_total", "tax_amount"]

query_list_total = {"OTHER": {
        "invoice_no": "Answer only invoice number",
        "po_no": "Answer only PO number",
        "invoice_date_epoch": "Answer only date into YYYY-MM-DD format or NONE",
        "vendor": "Answer only vendor name"
    }, "QUOTE": {
        "date_epoch": "Answer only due date into YYYY-MM-DD format",
        "quote_number": "Answer only quote number",
        "terms": "Answer only terms",
        "address": "Answer only address",
        "shipping_method": "Answer only ship method",
        "sub_total": "Answer only subtotal without $ symbol or 0",
        "tax": "Answer only tax without $ symbol or 0",
        "quote_total": "Answer only quote total without $ symbol or 0",
        "receiver_phone": "Answer only phone number",
        "vendor": "Answer only vendor name",
        "invoice_no": "Answer only invoice number",
    }, "INVOICE": {
        "customer_id": "Answer only customer id",
        "invoice_no": "Answer only invoice number",
        "po_no": "Answer only PO number",
        "invoice_date_epoch": "Answer only date into YYYY-MM-DD format",
        "due_date_epoch": "Answer only due date into YYYY-MM-DD format",
        "order_date_epoch": "Answer only order date into YYYY-MM-DD format",
        "ship_date_epoch": "Answer only ship date into YYYY-MM-DD format",
        "terms": "Answer only terms",
        "invoice_total_amount": "Answer only total without $ symbol or 0",
        "tax_amount": "Answer only tax without $ symbol or 0",
        "tax_id": "Answer only tax id",
        "sub_total": "Answer only subtotal without $ symbol or 0",
        "amount_due": "Answer only due amount without $ symbol or 0",
        "receiving_date_epoch": "Answer only receiving date into YYYY-MM-DD",
        "delivery_address": "Answer only delivery address",
        "contract_number": "Answer only phone number",
        "vendor": "Answer only vendor name",
    }, "PURCHASE_ORDER": {
        "date_epoch": "Answer only due date into YYYY-MM-DD format",
        "invoice_no": "Answer only invoice number",
        "po_no": "Answer only PO number",
        "customer_id": "Answer only customer id",
        "terms": "Answer only terms",
        "delivery_date_epoch": "Answer only delivery date into YYYY-MM-DD format",
        "delivery_address": "Answer only delivery address",
        "contract_number": "Answer only phone number",
        "quote_number": "Answer only quote number",
        "sub_total": "Answer only subtotal without $ symbol or 0",
        "tax": "Answer only tax without $ symbol or 0",
        "po_total": "Answer only total without $ symbol or 0",
        "vendor": "Answer only vendor name",
    }, "PACKING_SLIP": {
        "date_epoch": "Answer only date into YYYY-MM-DD format",
        "invoice_no": "Answer only invoice number",
        "ship_to_address": "Answer only ship to address",
        "vendor": "Answer only vendor name",
    }, "RECEIVING_SLIP": {
        "date_epoch": "Answer only  date into YYYY-MM-DD format",
        "invoice_no": "Answer only invoice number",
        "ship_to_address": "Answer only ship to address",
        "vendor": "Answer only vendor name",
        "po_no": "Answer only po number",
        "received_by": "Answer only receiver name",
    }}

# PATH /
# DESC TEST SERVER RUNNING


@app.route("/", methods=["GET"])
@cross_origin()
def home():
    return "Hello World !"


def get_fields(mydb, doc_type, filepath):
    
    result = {"document_type": doc_type}

    query_list = query_list_total[doc_type]

    for item in query_list:
        query = query_list[item]

        llm = OpenAI(
            temperature=0, openai_api_key=OPEN_AI_KEY)
        chain = load_qa_chain(llm, chain_type="stuff")

        with open('./JSON/vector-{}.json'.format(filepath), 'r') as infile:
            data = json.load(infile)
        print(type(data))
        embeddings = OpenAIEmbeddings(
            openai_api_key=OPEN_AI_KEY)

        query_result = embeddings.embed_query(query)
        query_results = np.array(query_result)
        doclist = utils.maximal_marginal_relevance(query_results, data)
        loader = CSVLoader(
            file_path='./CSV/index-{}.csv'.format(filepath), encoding="utf8")
        csv_text = loader.load()

        docs = []
        print(query)
        for res in doclist:
            docs.append(Document(
                page_content=csv_text[res].page_content, metadata=csv_text[res].metadata))

        result[item] = str(chain.run(input_documents=docs, question=query))
        result[item] = remove_first_space(result[item])

        if(result[item].find("I don't know") >= 0 ):
            result[item] = ""

        if(item.find("number") >= 0):
            result[item] = result[item].replace("-", "")
        
        if(item == "invoice_no" or item == "po_no"):
            result[item] = result[item].replace(" ", "")

        if(item == "vendor"):
            x = mydb[company_collections["VENDOR"]].find_one({"vendor_name": result[item]})
            if x != None:
                result[item] = x["_id"]
            else:
                result[item] = ""

        if(item.find("epoch") >= 0):
            result[item] = convert_epoch(result[item])
        try:
            list_to_convertINT.index(item)
        except:
            result[item] = result[item]
        else:
            try: 
                float(result[item]) 
            except:
                result[item] = result[item] 
            else:
                result[item] = float(result[item])  
    return result


@app.route("/process_invoice", methods=["POST"])
@cross_origin()
def process_invoice():
    count = {
        "PURCHASE_ORDER" : 0,
        "PACKING_SLIP" : 0,
        "RECEIVING_SLIP" : 0,
        "QUOTE" : 0,
        "INVOICE" : 0,
        "OTHER" : 0,
        "DUPLICATED" : 0
    }
    req_data = request.get_json()
    pdf_urls = req_data["pdf_urls"]
    company_code = req_data["company"]
    admin_col = admin_db[admin_collections["COMPANY"]]    
    Y = admin_col.find_one({"companycode": company_code})

    if Y == None:
        list_col.update_one({"_id" : ObjectId(id)}, {"$set" :{"status": "PROCESS_ERROR"}})
        return "Fail"
    
    mydb = myclient[Y["DB_NAME"]]
    list_col = mydb[company_collections["INVOICE_LIST"]]
    inserted_info = []

    for id in pdf_urls:
        X = list_col.find_one({"_id" : ObjectId(id)})
        if(X == None):
            continue
        
        parse_result = parse_file_path(X["pdf_url"])
        filename = parse_result["path"]
        bucket = parse_result["bucket"]
        region = parse_result["region"]
        endpoint = parse_result["endpoint"]

        # filepath = filename.replace("/", "-")
        filepath = id
        # Get Bytes Data From 'rovukdata'
        filebytes = get_object(WASABI_ACCESS_KEY, WASABI_SECRET_KEY, region,
                            endpoint, bucket, filename)
        textract = create_textract(
            AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        response = {}

        try:
            response = analyze_invoice(textract, filebytes)
        except:
            list_col.update_one({"_id" : ObjectId(id)}, {"$set" :{"status": "PROCESS_ERROR"}})
            continue
        type_doc = type_invoice(textract, filebytes)

        get_summary(response, filepath)

        embeding("./CSV/index-{}.csv".format(filepath), filepath)
        result = get_fields(mydb ,type_doc, filepath)
        table_items = get_table(response, filepath)

        result["items"] = table_items
        result["pdf_url"] = X["pdf_url"]
        result["document_id"] = id

        count[result["document_type"]] +=1

        if os.path.exists("./CSV/index-{}.csv".format(filepath)):
            os.remove("./CSV/index-{}.csv".format(filepath))

        if os.path.exists("./JSON/vector-{}.json".format(filepath)):
            os.remove("./JSON/vector-{}.json".format(filepath))

        inserted_id = schema_generator(mydb,result)
        inserted_obj = {"id": inserted_id}

        if "invoice_no" in result:
            inserted_obj["invoice_no"] = result["invoice_no"]
        else:
            inserted_obj["invoice_no"] = -1

        if "po_no" in result:
            inserted_obj["po_no"] = result["po_no"]
        else:
            inserted_obj["po_no"] = -1
        inserted_obj["document_type"] = result["document_type"]
        inserted_obj["vendor"] = result["vendor"]
        inserted_info.append(inserted_obj)

        dup_col = mydb[company_collections[result["document_type"]]]
        dup = dup_col.find({"vendor": result["vendor"], "invoice_no": result["invoice_no"]})
        if(len(list(dup)) >1):
            print(dup)
            count["DUPLICATED"] = count["DUPLICATED"] + 1
    # print(inserted_info)
    count_col = mydb[company_collections["API_COUNT"]]
    x = count_col.find_one({"year": datetime.now().year, "month": datetime.now().month})
    if x == None:
        count_obj = {"year": datetime.now().year, "month": datetime.now().month, "is_delete" : 0}
        for item in count:
            count_obj[item] = count[item]
        count_col.insert_one(count_obj)
    else:
        count_obj = {}
        for item in count:
            count_obj[item] = count[item] + x[item]

        count_col.update_one({"year": datetime.now().year, "month": datetime.now().month}, {"$set": count_obj})
        
    find_relationship(mydb, inserted_info)
    list_col.update_one({"_id" : ObjectId(id)}, {"$set" :{"status": "PROCESS_COMPLETE"}})
    return "Success"

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)