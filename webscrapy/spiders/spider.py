"""
Project: Web scraping for customer reviews
Author: Hào Cui
Date: 06/22/2023
"""
import json
import re

import scrapy
from scrapy import Request

from webscrapy.items import WebscrapyItem


class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["www.castorama.pl", "api.bazaarvoice.com"]
    headers = {} 

    def start_requests(self):
        # keywords = ['Stanley', 'Black+Decker', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Facom', 'MAC Tools', 'Vidmar', 'Lista', 'Irwin Tools', 'Lenox', 'Proto', 'CribMaster', 'Powers Fasteners', 'cub-cadet', 'hustler', 'troy-bilt', 'rover', 'BigDog Mower', 'MTD']
        exist_keywords = ['dewalt', 'Stanley', 'Black+Decker', 'Irwin',]
        
        # company = 'Stanley Black and Decker'
        # from search words to generate product_urls
        for keyword in exist_keywords:
            push_key = {'keyword': keyword}
            search_url = f'https://www.castorama.pl/search?term={keyword}'

            yield Request(
                url=search_url,
                callback=self.parse,
                cb_kwargs=push_key,
            )

    def parse(self, response, **kwargs):
        # extract the total number of product results
        page_number = int(re.search(r'"totalResults":(\d+)', response.body.decode('utf-8')).group(1))
        pages = (page_number // 24) + 1

        # Based on pages to build product_urls
        keyword = kwargs['keyword']
        product_urls = [f'https://www.castorama.pl/search?page={page}&term={keyword}' for page
                        in range(1, pages+1)]  #  pages+1

        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_parse, meta={'product_brand':keyword})

    def product_parse(self, response: Request, **kwargs):
        product_brand = response.meta['product_brand']
        
        # extract the product url link from each page of product list
        product_urls = re.findall(r'"shareableUrl":"(.*?)"', response.body.decode('utf-8'))

        for product_url in product_urls:
            product_detailed_url = product_url.encode().decode('unicode-escape')
            yield Request(url=product_detailed_url, callback=self.product_detailed_parse, meta={'product_brand':product_brand})

    def product_detailed_parse(self, response, **kwargs):
        product_brand = response.meta['product_brand']
        product_id = response.xpath('.//*[@id="product-details"]//td[@data-test-id="product-ean-spec"]/text()')[
            0].extract()
        product_name = response.xpath('//*[@id="product-title"]/text()')[0].extract()
        product_detail = response.xpath('.//tbody/tr')

        # extract product detail infor
        product_type = 'N/A'
        product_model = 'N/A'

        for product in product_detail:
            th_text = product.xpath('./th/text()')[0].extract()
            td_text = product.xpath('./td/text()').extract()
            if th_text == "Typ produktu":
                product_type = td_text[0] if td_text else 'N/A'
            elif th_text == 'Marka':
                product_brand = td_text[0] if td_text else 'N/A'
            elif th_text == 'Kod produktu':
                product_model = td_text[0] if td_text else 'N/A'

        # Product reviews url
        product_detailed_href = f'https://api.bazaarvoice.com/data/batch.json?passkey' \
                                f'=cauXqtM5OxUGSckj1VCPUOc1lnChnQoTYXBE5j082Xuc0&apiversion=5.5&displaycode=17031' \
                                f'-pl_pl&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter.q0=productid' \
                                f'%3Aeq%3A{product_id}&filter.q0=contentlocale%3Aeq%3Apl*%2Cpl_PL&sort.q0=rating' \
                                f'%3Adesc&stats.q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts' \
                                f'%2Ccomments&filter_reviews.q0=contentlocale%3Aeq%3Apl*%2Cpl_PL' \
                                f'&filter_reviewcomments.q0=contentlocale%3Aeq%3Apl*%2Cpl_PL&filter_comments.q0' \
                                f'=contentlocale%3Aeq%3Apl*%2Cpl_PL&limit.q0=8&offset.q0=0&limit_comments.q0=3 '

        if product_detailed_href:
            yield Request(url=product_detailed_href, callback=self.review_parse, meta={'product_name': product_name, 'product_type':product_type, 'product_brand':product_brand, 'product_model':product_model})

    def review_parse(self, response: Request, **kwargs):
        product_name = response.meta['product_name']
        product_type = response.meta['product_type']
        product_brand = response.meta['product_brand']
        product_model = response.meta['product_model']
        datas = json.loads(response.body)
        batch_results = datas.get('BatchedResults', {})
        offset_number = 0
        limit_number = 0
        total_number = 0
        if "q1" in batch_results:
            result_key = "q1"
        else:
            result_key = "q0"
        offset_number = batch_results.get(result_key, {}).get('Offset', 0)
        limit_number = batch_results.get(result_key, {}).get('Limit', 0)
        total_number = batch_results.get(result_key, {}).get('TotalResults', 0)

        for i in range(limit_number):
            item = WebscrapyItem()
            results = batch_results.get(result_key, {}).get('Results', [])
            try:
                item['review_id'] = results[i].get('Id', 'N/A')
                item['product_website'] = 'castorama_pl'
                item['product_name'] = product_name
                item['product_type'] = product_type
                item['product_brand'] = product_brand
                item['product_model'] = product_model
                item['customer_name'] = results[i]['UserNickname'] if results[i]['UserNickname'] else 'Anonymous'
                item['customer_rating'] = results[i].get('Rating', 'N/A')
                item['customer_date'] = results[i].get('SubmissionTime', 'N/A')
                item['customer_review'] = results[i].get('ReviewText', 'N/A')
                item['customer_support'] = results[i].get('TotalPositiveFeedbackCount', 'N/A')
                item['customer_disagree'] = results[i].get('TotalNegativeFeedbackCount', 'N/A')

                yield item
            except Exception as e:
                break

        if (offset_number + limit_number) < total_number:
            offset_number += limit_number
            next_page = re.sub(r'limit.q0=\d+&offset.q0=\d+', f'limit.q0={30}&offset.q0={offset_number}', response.url)
            yield Request(url=next_page, callback=self.review_parse, meta={'product_name': product_name, 'product_type':product_type, 'product_brand':product_brand, 'product_model':product_model})

