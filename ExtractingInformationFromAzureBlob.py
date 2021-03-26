import cv2,numpy,io,os
from pdf2image import convert_from_path,convert_from_bytes
import pyodbc
import Configurations as config
from azure.storage.blob._container_client import ContainerClient,BlobClient
import pytesseract as pt


def pil_to_cv2(image):
    open_cv_image = numpy.array(image)
    return open_cv_image[:, :, ::-1].copy()


blobname1='Sample-1.pdf'
blobname2='Sample-2.pdf'
blobs = []

blob_service_client = ContainerClient.from_connection_string(config.myconnectionstring,config.containername)
print("\nBlobs Lists..")
blob_list=blob_service_client.list_blobs()
for blob in blob_list:
    print("\t" + blob.name)
    b=blob.name
    blob_client = blob_service_client.get_blob_client(b)
    b = blob_client.download_blob(offset=None, length=None)
    bytedata = b.readall()
    if blob.name == blobname1:
        pages = convert_from_bytes(bytedata)
        for page in pages:
            cv_h = pil_to_cv2(page)
            image_resize = cv2.resize(cv_h, (1400, 1400))
        Cropping_coordinates = [[(1027, 70), (239, 28), 'Billing Date'],
                                [(1010, 112), (286, 67), 'Paying Date'],
                                [(1173, 225), (111, 24), 'Account Number'],
                                [(1153, 274), (125, 16), 'PRN'],
                                [(104, 77), (359, 67), 'Client Name']]
        myData1 = []
        for r in Cropping_coordinates:
            imgcrop = image_resize[r[0][1]: r[0][1] + r[1][1], r[0][0]:r[0][0] + r[1][0]]
            if r[2] == 'Billing Date':
                Bd = myData1.append(pt.image_to_string(imgcrop))
            elif r[2] == 'Paying Date':
                myData1.append(pt.image_to_string(imgcrop))
            elif r[2] == 'Account Number':
                myData1.append(pt.image_to_string(imgcrop))
            elif r[2] == 'PRN':
                myData1.append(pt.image_to_string(imgcrop))
            elif r[2] == 'Client Name':
                myData1.append(pt.image_to_string(imgcrop))
        print(myData1)
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=.;'
                              'Database=StoringData;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        cursor.execute(
            'CREATE TABLE Business1 (BillingDate nvarchar(50),PayingDate nvarchar(50),AccountNumber nvarchar(50),PRN nvarchar(50),ClientName nvarchar(50))')
        query = ("insert into StoringData.dbo.Business1(BillingDate, PayingDate, AccountNumber, PRN, ClientName) "
                 "values (?, ?, ?, ?,?)")
        cursor.execute(query, myData1[0], myData1[1], myData1[2], myData1[3], myData1[4])
        conn.commit()
    elif blob.name == blobname2:

        pages = convert_from_bytes(bytedata)
        for page in pages:
            cv_h = pil_to_cv2(page)
            image_resize = cv2.resize(cv_h, (1000, 1131))
        Cropping_coordinates = [[(451, 90), (346, 74), 'Supplier Name'],
                                [(576, 381), (142, 41), 'Supplier VAT Number'],
                                [(588, 152), (138, 38), 'Invoice Number'],
                                [(221, 175), (166, 34), 'Invoice Billing Date'],
                                [(490, 266), (246, 88), 'Supplier Address']]
        myData2 = []
        for r in Cropping_coordinates:

            imgcrop = image_resize[r[0][1]: r[0][1] + r[1][1], r[0][0]:r[0][0] + r[1][0]]
            if r[2] == 'Supplier Name':
                bd = myData2.append(pt.image_to_string(imgcrop, config=('-l eng --oem 1 --psm 8')))
            elif r[2] == 'Supplier VAT Number':
                myData2.append(pt.image_to_string(imgcrop))
            elif r[2] == 'Invoice Number':
                myData2.append(pt.image_to_string(imgcrop))
            elif r[2] == 'Invoice Billing Date':
                myData2.append(pt.image_to_string(imgcrop).replace(',', ''))
            elif r[2] == 'Supplier Address':
                myData2.append(pt.image_to_string(imgcrop).replace(',', '').split(','))
        print(myData2)
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=.;'
                              'Database=StoringData;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        cursor.execute(
            'CREATE TABLE Business2 (SupplierName nvarchar(50),SupplierVATNumber nvarchar(50),InvoiceNumber nvarchar(50),InvoiceBillingDate nvarchar(50))')
        query1 = (
            "insert into StoringData.dbo.Business2(SupplierName,SupplierVATNumber, InvoiceNumber, InvoiceBillingDate) "
            "values (?, ?, ?, ?)")
        cursor.execute(query1, myData2[0], myData2[1], myData2[2], myData2[3])

        conn.commit()