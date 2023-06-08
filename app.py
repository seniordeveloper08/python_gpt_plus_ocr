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
    "Invoice": "ap_invoices", 
    "Purchase Order": "ap_pos", 
    "Packing Slip": "ap_packagingslips", 
    "Receiving Slip": "ap_receivingslips",
    "Invoice List": "ap_document_processes",
    "QUOTE": "ap_quotes",
    "OTHER": "ap_otherdocuments",
    "VENDOR": "invoice_vendors",
    "API_COUNT": "api_count"
}

# PATH /
# DESC TEST SERVER RUNNING


@app.route("/", methods=["GET"])
@cross_origin()
def home():
    return "Hello World !"


def get_fields(mydb, doc_type, filepath):
    query_list_total = {"OTHER": {
        "invoice_no": "invoice number if there is no correct value return NONE",
        "po_no": "PO number if there is no correct value return NONE",
        "invoice_date_epoch": "Convert date into YYYY-MM-DD format without any string",
        "vendor": "vendor name if there is no correct value return NONE"
    }, "QUOTE": {
        "date_epoch": "Convert due date into YYYY-MM-DD format without any string",
        "quote_number": "quote number if there is no correct value return NONE",
        "terms": "If there is value for terms, print it or if not, print NONE",
        "address": "address if there is no correct value return NONE",
        "shipping_method": "ship method",
        "sub_total": "subtotal without $ symbol if there is no correct value return NONE",
        "tax": "tax without $ symbol if there is no correct value return NONE",
        "quote_total": "total without $ symbol if there is no correct value return NONE",
        "receiver_phone": "phone number if there is no correct value return NONE",
        "vendor": "vendor name if there is no correct value return NONE"
    }, "Invoice": {
        "customer_id": "customer id if there is no correct value return NONE",
        "invoice_no": "invoice number if there is no correct value return NONE",
        "po_no": "PO number if there is no correct value return NONE",
        "invoice_date_epoch": "Convert date into YYYY-MM-DD format without any string",
        "due_date_epoch": "Convert due date into YYYY-MM-DD format without any string",
        "order_date_epoch": "Convert order date into YYYY-MM-DD format without any string",
        "ship_date_epoch": "Convert ship date into YYYY-MM-DD format without any string",
        "terms": "If there is value for terms, print it or if not, print NONE",
        "invoice_total_amount": "total without $ symbol if there is no correct value return NONE",
        "tax_amount": "tax without $ symbol if there is no correct value return NONE",
        "tax_id": "If there is value for tax id, print it or if not, print NONE",
        "sub_total": "subtotal without $ symbol if there is no correct value return NONE",
        "amount_due": "due amount without $ symbol if there is no correct value return NONE",
        "receiving_date_epoch": "Convert receiving date into YYYY-MM-DD format without any string",
        "delivery_address": "delivery address if there is no correct value return NONE",
        "contract_number": "phone number if there is no correct value return NONE",
        "vendor": "vendor name if there is no correct value return NONE",
        # "TO": "Bill, to:",
        # "SHIP-TO": "ship to:",
        # "VENDOR-NAME": "vendor name if there is no correct value return NONE",
        # "VENDOR-ADDRESS": "vendor address if there is no correct value return NONE",
        # "PHONE-NUMBER": "phone number if there is no correct value return NONE"
    }, "Purchase Order": {
        # "INVOICE-NUMBER": "invoice number if there is no correct value return NONE",
        "date_epoch": "Convert date into YYYY-MM-DD format without any string",
        "po_no": "PO number if there is no correct value return NONE",
        "customer_id": "customer id if there is no correct value return NONE",
        "terms": "terms if there is no correct value return NONE",
        "delivery_date_epoch": "Convert delivery date into YYYY-MM-DD format without any string",
        "delivery_address": "delivery address if there is no correct value return NONE",
        "contract_number": "phone number if there is no correct value return NONE",
        "quote_number": "quote number if there is no correct value return NONE",
        # "TO": "Bill to:",
        # "SHIP-TO": "ship to:",
        "sub_total": "subtotal without $ symbol if there is no correct value return NONE",
        "tax": "tax without $ symbol if there is no correct value return NONE",
        "po_total": "total without $ symbol if there is no correct value return NONE",
        "vendor": "vendor name if there is no correct value return NONE",
        # "VENDOR-ADDRESS": "vendor address if there is no correct value return NONE",
        # "PHONE-NUMBER": "phone number if there is no correct value return NONE"
    }, "Packing Slip": {
        "date_epoch": "Convert date into YYYY-MM-DD format without any string",
        "invoice_no": "invoice number if there is no correct value return NONE",
        "ship_to_address": "ship to address",
        "vendor": "vendor name if there is no correct value return NONE",
        # "VENDOR-ADDRESS" : "vendor address if there is no correct value return NONE",
        # "po_number" : "PO number if there is no correct value return NONE",
        # "RECEIVER-NAME" : "RECEIVER NAME if there is no correct value return NONE",

    }, "Receiving Slip": {
        "date_epoch": "Convert date into YYYY-MM-DD format",
        "invoice_no": "number",
        "ship_to_address": "ship to address",
        "vendor": "vendor name if there is no correct value return NONE",
        # "VENDOR-ADDRESS" : "vendor address without vendor name",
        "po_no": "number",
        "received_by": "RECEIVER NAME if there is no correct value return NONE",

    }}

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

        if(item.find("number") >= 0):
            result[item] = result[item].replace("-", "")
        
        if(item == "invoice_no" or item == "po_no" or item == "po_number" or item == "invoice_number"):
            result[item] = result[item].replace(" ", "")

        if(item == "vendor"):
            x = mydb[company_collections["VENDOR"]].find_one({"vendor_name": result[item]})
            if x != None:
                result[item] = x["_id"]
            else:
                result[item] = ""

        if(item.find("epoch") >= 0):
            result[item] = convert_epoch(result[item])
    return result


@app.route("/process_invoice", methods=["POST"])
@cross_origin()
def process_invoice():
    req_data = request.get_json()
    count = 0
    pdf_urls = req_data["pdf_urls"]
    company_code = req_data["company"]

    admin_col = admin_db[admin_collections["COMPANY"]]
    Y = admin_col.find_one({"companycode": company_code})
    if Y == None:
        return "Fail"
    
    mydb = myclient[Y["DB_NAME"]]

    list_col = mydb[company_collections["Invoice List"]]
    inserted_info = []

    for id in pdf_urls:
        X = list_col.find_one({"_id" : ObjectId(id)})

        if(X == None):
            continue
        
        parse_result = parse_file_path(X["pdf_url"])
        count +=1
        filename = parse_result["path"]
        bucket = parse_result["bucket"]
        region = parse_result["region"]
        endpoint = parse_result["endpoint"]

        filepath = filename.replace("/", "-")

        # Get Bytes Data From 'rovukdata'
        filebytes = get_object(WASABI_ACCESS_KEY, WASABI_SECRET_KEY, region,
                            endpoint, bucket, filename)

        textract = create_textract(
            AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        response = analyze_invoice(textract, filebytes)
        type_doc = type_invoice(textract, filebytes)

        get_summary(response, filepath)

        embeding("./CSV/index-{}.csv".format(filepath), filepath)
        result = get_fields(mydb ,type_doc, filepath)
        table_items = get_table(response, filepath)

        result["items"] = table_items
        result["pdf_url"] = X["pdf_url"]
        result["document_id"] = id

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
    # print(inserted_info)
    count_col = mydb[company_collections["API_COUNT"]]
    x = count_col.find_one({"companycode": company_code})
    if x == None:
        count_col.insert_one({"companycode": company_code, "company": Y["_id"], "count": count})
    else:
        count = count + x["count"]
        count_col.update_one({"companycode": company_code}, {"$set": {"count": count}})
    find_relationship(mydb, inserted_info)
    return "Success"



if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
