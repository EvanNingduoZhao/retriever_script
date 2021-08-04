from os import pardir
import numpy as np
import pandas as pd
import time
import requests
# from google.colab import auth
from google.cloud import bigquery
import json
from data import MERCH_PILOT_FULL

class BigQueryDAO():
    """A DAO class for connecting to Big Query."""

    def __init__(self, interface='colab', project='wf-gcp-us-ae-sf-prod'):

        if interface=='jenkins':
            import wf_secrets
            creds_name = 'wf-us-ae-seo-svc-botify'
            creds_raw = wf_secrets.get(creds_name)
            creds = loads(creds_raw)
            if not creds:
                print(f"There was an error getting {creds_name} from wf_secrets")
                exit(1)
            with open('key.json', 'w') as fp:
                dump(creds, fp)
            self.client = bigquery.Client.from_service_account_json('key.json')
        elif interface=='colab':
            self.client = bigquery.Client(project)

    def setup(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS
            `wf-gcp-us-ae-sf-prod.seo.tbl_visual_similarity_scores`
            (URL STRING NOT NULL, Keyword STRING NOT NULL, Score FLOAT64 NOT NULL, EventDate DATE NOT NULL);
        """
        table = self.client.query(query).done()

    def insert(self, values):
        url, keyword, score, job_id = values
        query = f"""
            INSERT INTO `wf-gcp-us-ae-sf-prod.seo.tbl_visual_similarity_scores`
                (URL, Keyword, Score, EventDate, JobID)
            VALUES ('{url}', '{keyword}', {score}, CURRENT_DATE(), '{job_id}')
        """
        result = self.client.query(query).done()

    def get_table(self, table, dataset='seo', project='wf-gcp-us-ae-sf-prod'):
      query = f"""
          SELECT *
          FROM `{project}.{dataset}.{table}`
      """
      return self.client.query(query).to_dataframe()


class ScrapedDataRetriever:
  def __init__(self, keyword_url_pairs):
    self.bq = BigQueryDAO()
    self.keyword_url = keyword_url_pairs
    self.data = {}

  def divide_to_chunk(self):
    # divide input keyword_url_pairs into chunks with each chunk of size 100 pairs
    chunks = []
    startIndex = 0
    while startIndex < len(self.keyword_url):
      endIndex = min(startIndex+100, len(self.keyword_url))
      chunks.append(self.keyword_url[startIndex:endIndex])
      startIndex=endIndex
    chunks_total_length = 0
    for chunk in chunks:
      chunks_total_length+=len(chunk)
    print("Divided input keyword&urls into {num_of_chunks} chunks with the total length of {total_length}".format(
      num_of_chunks=len(chunks),
      total_length=(chunks_total_length)))
    return chunks

  
  def get_keyword_page_info(self, chunk):
    urlList = ""
    for pair in chunk:
      urlList+="\'"
      urlList+=pair[1]
      urlList+="\',"
    # remove the last comma from urlList string
    urlList = urlList[:-1]

    # keywordPageKeyPair = {}
    # keywordPageKeyPair["Keyword"] = keyword
    getPageKeyQuery = f"""
    select url, SoID, BclgID, PageID, PageTypeID, FilterString from `wf-gcp-us-ae-bulk-prod.csn_seo.tbl_seo_urls`
    where url in ({urlList})
    """
    pageKeys = self.bq.client.query(getPageKeyQuery).to_dataframe().to_dict("records")
    print("Query for current chunk return {num_of_record} records".format(num_of_record=len(pageKeys)))
    if pageKeys and len(pageKeys)>0:
      for record in pageKeys:
        self.data[record["url"]]["SoID"]=record["SoID"]
        self.data[record["url"]]["BclgID"]=record["BclgID"]
        self.data[record["url"]]["PageID"]=record["PageID"]
        self.data[record["url"]]["PageTypeID"]=record["PageTypeID"]
        self.data[record["url"]]["FilterString"]=record["FilterString"]
        print("Just added a record to self.data: ",self.data[record["url"]])
      # keywordPageKeyPair["SoID"] = int(pageKey["SoID"][0])
      # keywordPageKeyPair["BclgID"] = int(pageKey["BclgID"][0])
      # keywordPageKeyPair["PageID"] = pageKey["PageID"][0]
      # keywordPageKeyPair["PageTypeID"] = int(pageKey["PageTypeID"][0])
      # keywordPageKeyPair["FilterString"] = pageKey["FilterString"][0]
      # return keywordPageKeyPair
    else:
      print("Something is wrong: Query result is empty!")

  
  def get_serp_data(self, keyword):
    getSerpDataQuery = f"""
    select eventdate, img_b64 from `wf-gcp-us-ae-sf-prod.seo.tbl_image_scrape_serp` where keyword="{keyword}" and eventdate = (select MAX(eventdate) from `wf-gcp-us-ae-sf-prod.seo.tbl_image_scrape_serp` where keyword="{keyword}" )
    """
    serpData = self.bq.client.query(getSerpDataQuery).to_dataframe()
    serpImgs, serpDate = [], None
    if not serpData.empty:
      serpImgs = serpData['img_b64'].tolist()
      # serpDate = serpData['eventdate'][0]
      return serpImgs
    else:
      print("Can't find scraped SERP img data for keyword: ", keyword)
      return None


  def get_site_data(self, keyword):
    getSiteDataQuery = f"""
    select eventdate, img_b64 from `wf-gcp-us-ae-sf-prod.seo.tbl_image_scrape_site` where keyword="{keyword}" and eventdate = (select MAX(eventdate) from `wf-gcp-us-ae-sf-prod.seo.tbl_image_scrape_site` where keyword="{keyword}" )
    """
    siteData = self.bq.client.query(getSiteDataQuery).to_dataframe()
    siteImgs, siteDate = [], None
    if not siteData.empty:
      siteImgs = siteData['img_b64'].tolist()
      siteDate = siteData['eventdate'][0].strftime("%Y-%m-%d")
      return siteImgs, siteDate
    else:
      print("Can't find scraped wayfair site img data for keyword: ", keyword)
      return None, None
  
  def retrieve_data_by_chunk(self):
    chunks = self.divide_to_chunk()
    # remember to change it back to chunks
    for chunk in chunks[:2]:
      # store data using a dict of dict, key of outer dict should be url
      for pair in chunk:
        url = pair[1]
        self.data[url]={}
      # after initialized a dict for each url in self.data, call a series
      # of the query methods to batch query the whole chunk
      self.get_keyword_page_info(chunk)

  
  def form_request_body(self, url_keyword_pairs):
    requestBody = {"keyword_page_info":[], "image_data":[]}
    for url, keyword in url_keyword_pairs:
      print("####Processing keyword: ", keyword)
      individual_keyword_page_info = self.get_keyword_page_info(keyword,url)
      serpImgs = self.get_serp_data(keyword)
      siteImgs, siteDate = self.get_site_data(keyword)
      if individual_keyword_page_info and serpImgs and siteImgs and siteDate:
        requestBody["keyword_page_info"].append(individual_keyword_page_info)
        individual_img_data = {"wayfair_b64_imgs":siteImgs,"google_serp_b64_imgs":serpImgs, "time": siteDate}
        requestBody["image_data"].append(individual_img_data)
      else:
        print("something required to form request body for this keyword is missing")
        continue
    return json.dumps(requestBody)


  def send_request(self, url_keyword_pairs):
    requestBody = json.loads(self.form_request_body(url_keyword_pairs))
    print(requestBody)
    response = requests.post(url='https://kube-seo-visual-similarity-scoring-service.service.intraiad1.consul.csnzoo.com/calculate_score', json=requestBody)
    print(response.status_code, response.content)



# auth.authenticate_user()
url_keyword_pairs = MERCH_PILOT_FULL
retriever = ScrapedDataRetriever(url_keyword_pairs)
retriever.retrieve_data_by_chunk()
# url_keyword_pairs = [("https://www.wayfair.com/keyword.php?keyword=king+size+car+bed","king size car bed"),("https://www.wayfair.com/keyword.php?keyword=indoor+stair+railings+kits","indoor stair railings kits")]
# retriever.send_request(url_keyword_pairs)