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
    headers = {}  #

    def start_requests(self):
        # keywords = ['DeWalt', 'Black+and+Decker', 'Stanley', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Irwin+Tools',
        #             'Lenox']
        # company = 'Stanley Black and Decker'

        keywords = ['dewalt']
        # from search words to generate product_urls
        for keyword in keywords:
            push_key = {'keyword': keyword}
            search_url = f'https://www.castorama.pl/search?term={keyword}'

            yield Request(
                url=search_url,
                callback=self.parse,
                cb_kwargs=push_key,
                # meta={'proxy':'socks5://127.0.0.1:10110'},
                # headers=self.headers
            )

    def parse(self, response, **kwargs):

        # Extract the pages of product_urls
        page = response.xpath('//p[@data-test-id="search-options-total-results"]/text()')[0].extract()
        page_number = int(''.join(filter(str.isdigit, page)))
        pages = (page_number // 24) + 1

        # Based on pages to build product_urls
        keyword = kwargs['keyword']
        product_urls = [f'https://www.castorama.pl/search?page={page}&term={keyword}' for page
                        in range(1, 2)]  #pages+1

        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_parse)

    def product_parse(self, response: Request, **kwargs):

        product_list = response.xpath('//*[@id="content"]//main//ul/li')

        for product in product_list:
            product_href = product.xpath('.//div[@data-test-id="product-panel"]/a/@href')[0].extract()
            product_detailed_url = f'https://www.castorama.pl{product_href}'
            yield Request(url=product_detailed_url, callback=self.product_detailed_parse)

    def product_detailed_parse(self, response, **kwargs):

        product_id = response.xpath('.//*[@id="product-details"]//td[@data-test-id="product-ean-spec"]/text()')[
            0].extract()
        product_name = response.xpath('//*[@id="product-title"]/text()')[0].extract()

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
            yield Request(url=product_detailed_href, callback=self.review_parse, meta={'product_name': product_name})

    def review_parse(self, response: Request, **kwargs):
        product_name = response.meta['product_name']
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
                item['product_name'] = product_name
                item['customer_name'] = results[i]['UserNickname'] if results[i]['UserNickname'] else 'Anonymous'
                item['customer_rating'] = results[i].get('Rating', 'N/A')
                item['customer_date'] = results[i].get('SubmissionTime', 'N/A')
                item['customer_review'] = results[i].get('ReviewText', 'N/A')
                item['customer_support'] = results[i].get('TotalPositiveFeedbackCount', 'N/A')
                item['customer_disagree'] = results[i].get('TotalNegativeFeedbackCount', 'N/A')

                yield item
            except Exception as e:
                print('Exception:', e)
                break

        if (offset_number + limit_number) < total_number:
            offset_number += limit_number
            next_page = re.sub(r'limit.q0=\d+&offset.q0=\d+', f'limit.q0={30}&offset.q0={offset_number}', response.url)
            yield Request(url=next_page, callback=self.review_parse)

