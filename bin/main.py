#!/usr/bin/env python
"""
coding=utf-8

Code template courtesy https://github.com/bjherger/Python-starter-repo

"""
import logging
from urllib.parse import urljoin

import pandas
import requests
from bs4 import BeautifulSoup

from bin import lib


def main():
    """
    Main function documentation template
    :return: None
    :rtype: None
    """
    logging.basicConfig(level=logging.DEBUG)

    # Reference variables
    scrape_forum_config = False
    parse_forum_config = False
    scrape_threads_config = True
    parse = True

    # Scrape pages from forum & archive
    if scrape_forum_config:
        scrape_forum('united-airlines-mileageplus-681')

    # Parse the forum pages
    if parse_forum_config:
        parse_forum()

    if scrape_threads_config:
        scrape_threads()
    # TODO Scrape posts from forum & archive

    # TODO Parse results & archive
    pass

def scrape_forum(forum_name):
    """
    Walk a forum page by page, and pull each page, including:

     - Page number
    :return: Pandas dataframe with the above
    """
    # Reference variables
    base_url = 'https://www.flyertalk.com/forum/'
    forum_url = urljoin(base_url, forum_name)
    logging.info('base_url: {}, forum_name: {}, forum_url: {}'.format(base_url, forum_name, forum_url))
    results = list()

    # Create list of page_urls to parse, and add in first page (which is formatted differently)
    page_urls = list(map(lambda x: '{}-{}.html'.format(forum_url, x), range(2, 1323)))
    page_urls = [forum_url] + page_urls
    logging.info('Page_urls: {}'.format(page_urls[:3]))

    for index, page_url in enumerate(page_urls, start=1):
        logging.info('Attempting to parse index: {}, with page_url: {}'.format(index, page_url))
        lib.wait()
        result_dict = dict()
        result_dict['page_url'] = page_url
        result_dict['index'] = index
        try:
            r = requests.get(page_url)
            result_dict['page_html'] = r.content
            result_dict['http_status'] = r.status_code
            result_dict['error'] = None
        except Exception as e:
            logging.warning('Saw error: {}'.format(str(e)))
            result_dict['error'] = str(e)

        results.append(result_dict)

    forum_pages = pandas.DataFrame(results)
    forum_pages.to_pickle('../data/output/forum_pages.pkl')
    print(forum_pages['page_html'])

    return forum_pages

def parse_forum():
    # Reference variables
    forum_pages = pandas.read_pickle('../data/output/forum_pages.pkl')
    results = list()

    # Step through each forum page (listing multiple threads)
    for content in forum_pages['page_html']:
        soup = BeautifulSoup(content)

        # Step through each link on the page
        for thread_result in soup.find_all('a'):
            thread_id = thread_result.get('id')

            # If the link describes a thread, parse that link
            if thread_id is not None and str(thread_id).startswith('thread_title_'):
                logging.debug('Working thread_id: {}'.format(thread_id))
                result_dict = dict()
                result_dict['text'] = thread_result.get_text()
                result_dict['url'] = thread_result.attrs['href']
                result_dict['id'] = str(thread_id)
                result_dict['thread_id'] = str(thread_id).replace('thread_title_', '')

            results.append(results)

    threads = pandas.DataFrame(results)
    threads.to_pickle('../data/output/threads.pkl')
    return threads

def scrape_threads():
    threads = pandas.read_pickle('../data/output/threads.pkl')

    for url in threads['url']:
        logging.debug('Parsing url: {}'.format(url))

        # TODO Determine number of pages, from first page
        # TODO Create a list of pages to work through
        # TODO Iterate through pages, and pull each one
        pass


    pass

# Main section
if __name__ == '__main__':
    main()
