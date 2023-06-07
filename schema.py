import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

collections = {
    "Invoice": "ap_invoices", 
    "Purchase Order": "ap_pos", 
    "Packing Slip": "ap_packagingslips", 
    "Receiving Slip": "ap_receivingslips",
    "QUOTE": "ap_quotes",
    "OTHER": "ap_otherdocuments"
}

def schema_generator(mydb, schema_param):
    schemas = {
        "Packing Slip" : {
            "invoice_id": "",
            "pdf_url": "", # Wasabi s3 bucket packaging slip document url
            "document_id": "", # Process document id
            "document_type": "", # Process document type
            "date_epoch": 0,
            "invoice_no": "",
            "po_no": "",
            "ship_to_address": "",
            "vendor": "", # Vendor Collection - Vendor Id
            "is_delete": 0,
            "is_orphan": 0, # 0 - Orphan document, 1 - already relationship with invoice document
            "created_by": "",
            "created_at": 0,
            "updated_by": "",
            "updated_at": 0,
        }, "Invoice" : {
            "assign_to": "", #user collection ID - All By default empty like rillion
            "vendor": "", # vendor collection ID
            # "vendor_name": "", # Vendor collection
            "is_quickbooks": False, # This is for future for Quickbooks sync
            "vendor_id": "", # vendor collection
            "customer_id": "", # vendor collection
            "invoice_no": "",
            "po_no": "",
            "packing_slip_no": "",
            "receiving_slip_no": "",
            "invoice_date_epoch": 0, # Epoch 
            "due_date_epoch": 0,
            "order_date_epoch": 0,
            "ship_date_epoch": 0,
            "terms": "", # Vendor terms OR coming from Terms Setting master : ID
            "invoice_total_amount": 0,
            "tax_amount": 0,
            "tax_id": "",
            "sub_total": 0,
            "amount_due": 0,
            "gl_account": "", # Coming from settings costcode/glaccount table  - Job # ID
            "receiving_date_epoch": 0,
            "status": "Pending",
            # { type: String, default: "Pending", enum: ['Pending', 'Approved', 'Rejected', 'On Hold', 'Late', 'Paid', 'Unpaid', 'Overdue'] },
            "reject_reason": "",
            "job_client_name": "", # Coming from job client name Side menu - collection name is ID,
            "class_name": "", # ID
            "delivery_address": "",
            "contract_number": "",
            "account_number": "",
            "discount": "",
            "pdf_url": "", # Wasabi s3 bucket invoice document url
            "items": [], # This will be the list of items inside the invoice document,
            "notes": "",
            "invoice_notes": [], # Notes Schema Array
            "document_type": "INVOICE", # Fixed Invoice document type
            "created_by": "",  # User collection ID
            "created_at": 0, # Epoch Date - When action taken
            "updated_by": "",  # User collection ID
            "updated_at": 0, # Epoch Data - When action taken
            "is_delete": 0, # 0 - for not archive, 1 - for archive   
        }, "Purchase Order" : {
            "invoice_id": "",
            "pdf_url": "", # Wasabi s3 bucket po document url
            "document_id": "", # Process document id
            "document_type": "", # Process document type PO
            "date_epoch": 0,
            "po_no": "",
            "customer_id": "",
            "terms": "",
            "delivery_date_epoch": 0,
            "delivery_address": "",
            "due_date_epoch": 0,
            "quote_number": "",
            "contract_number": "",
            "vendor_id": "",
            "vendor": "",
            "sub_total": 0,
            "tax": 0,
            "po_total": 0,
            "items": [],
            "is_delete": 0,
            "is_orphan": 0, # 0 - Orphan document, 1 - already relationship with invoice document
            "created_by": "",
            "created_at": 0,
            "updated_by": "",
            "updated_at": 0
        }, "Receiving Slip" : {
            "invoice_id": "",
            "pdf_url": "", # Wasabi s3 bucket receiving slip document url
            "document_id": "", # Process document id
            "document_type": "", # Process document type
            "date_epoch": 0,
            "invoice_no": "",
            "po_no": "",
            "ship_to_address": "",
            "vendor": "", # Vendor Collection - Vendor Id
            "received_by": "",
            "is_delete": 0,
            "is_orphan": 0, # 0 - Orphan document, 1 - already relationship with invoice document
            "created_by": "",
            "created_at": 0,
            "updated_by": "",
            "updated_at": 0,            
        }, "Notes" : {
            "notes": "",
            "created_at": 0,
            "created_by": "",
            "updated_at": 0,
            "updated_by": "",
            "is_delete": 0,
        }, "OTHER" : {
            "pdf_url": "", # Wasabi s3 bucket receiving slip document url
            "document_id": "", # Process document id
            "document_type": "", # Process document type
            "date_epoch": 0,
            "invoice_no": "",
            "po_no": "",
            "vendor": "", # Vendor Collection - Vendor Id
            "is_delete": 0,
            "is_orphan": 0, # 0 - Orphan document, 1 - already relationship with invoice document
            "created_by": "",
            "created_at": 0,
            "updated_by": "",
            "updated_at": 0,
        }, "QUOTE" : {
                "invoice_id": "",
                "document_id": "", # Process document id

                # Available Part to be save from result of OCR
                "pdf_url": "", # Wasabi s3 bucket quote document url
                "document_type": "", # Process document type
                "date_epoch": 0,
                "quote_number": "",
                "terms": "",
                "address": "",
                "vendor": "", # Vendor Collection - Vendor Id
                "shipping_method": "",
                "sub_total": 0,
                "tax": 0,
                "quote_total": 0,
                "receiver_phone": "",
                "items": [],

                "is_delete": 0,
                "is_orphan": True, # 0 - Orphan document, 1 - already relationship with invoice document
                "created_by": "",
                "created_at": 0,
                "updated_by": "",
                "updated_at": 0
            }
    } 
    schema_obj = {}
    schema_obj = schemas[schema_param["document_type"]]
    for x in schema_param:
        schema_obj[x] = schema_param[x]

    mycol = mydb[collections[schema_param["document_type"]]]
    x = mycol.insert_one(schema_obj)
    
    return x.inserted_id

def find_relationship(mydb, params):
    for item in params:
        if(item["document_type"] == "Packing Slip"):
            x = mydb[collections["Invoice"]].find_one({"invoice_no" : item["invoice_no"], "vendor" : item["vendor"]})
            if(x != None):
                mydb[collections["Packing Slip"]].update_one({"_id": item["id"]}, {"$set": {"invoice_id": x["_id"], "po_no": x["po_no"]}})

        if(item["document_type"] == "Receiving Slip"):
            x = mydb[collections["Invoice"]].find_one({"invoice_no" : item["invoice_no"], "vendor" : item["vendor"]})
            if(x != None):
                mydb[collections["Receiving Slip"]].update_one({"_id": item["id"]}, {"$set": {"invoice_id": x["_id"], "po_no": x["po_no"]}})

        if(item["document_type"] == "OTHER"):
            x = mydb[collections["Invoice"]].find_one({"invoice_no" : item["invoice_no"], "vendor" : item["vendor"]})
            if(x != None):
                mydb[collections["OTHER"]].update_one({"_id": item["id"]}, {"$set": {"invoice_id": x["_id"], "po_no": x["po_no"]}})
        
        if(item["document_type"] == "Purchase Order"):
            x = mydb[collections["Invoice"]].find_one({"po_no" : item["po_no"], "vendor" : item["vendor"]})
            if(x != None):
                mydb[collections["Purchase Order"]].update_one({"_id": item["id"]}, {"$set": {"invoice_id": x["_id"]}})   