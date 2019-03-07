#!/usr/bin/env python
"""
coding=utf-8

Code template courtesy https://github.com/bjherger/Python-starter-repo

"""
import glob
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
    scrape_threads_config = False
    parse_threads_config = False
    agg_results_config = True

    # Scrape pages from forum & archive
    if scrape_forum_config:
        scrape_forum('united-airlines-mileageplus-681')

    # Parse the forum pages
    if parse_forum_config:
        parse_forum()

    if scrape_threads_config:
        scrape_threads()

    if parse_threads_config:
        parse_threads()

    if agg_results_config:
        agg_results()
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
    s = requests.Session()
    results = list()

    num_urls = len(threads['url'])
    logging.info('num_urls: {}'.format(num_urls))

    # TODO remove subsetting for first 3 pages
    for url_index, url in enumerate(threads['url'], start=1):

        try:
            logging.info('Working url_index: {} of {} url: {}'.format(url_index, num_urls, url))

            thread_base_url = str(url).replace('.html', '')

            r = s.get(url)

            # Determine number of pages, from first page
            first_page_soup = BeautifulSoup(r.content)
            mb_page_text = first_page_soup.find_all(id='mb_page')[0].get_text()
            max_page_number = int(str(mb_page_text).replace('1 / ', ''))
            logging.debug('Max page number: {} for thread url: {}'.format(max_page_number, url))
            page_urls = list(map(lambda x: '{}-{}.html'.format(thread_base_url, x), range(2, max_page_number + 1)))
            page_urls = [url] + page_urls

            for index, page_url in enumerate(page_urls, start=1):
                logging.info('Attempting to parse index: {}, with page_url: {}'.format(index, page_url))
                lib.wait()

                result_dict = dict()
                result_dict['page_url'] = page_url
                result_dict['thread_url'] = url
                result_dict['thread_page_id'] = index
                result_dict['max_thread_page_id'] = max_page_number

                try:
                    r = requests.get(page_url)
                    result_dict['page_html'] = r.content
                    result_dict['http_status'] = r.status_code
                    result_dict['error'] = None
                except Exception as e:
                    logging.warning('Saw error: {}'.format(str(e)))
                    result_dict['error'] = str(e)

                results.append(result_dict)
        except:
            logging.warning('Error w/ {}, {}'.format(url_index, url))

        if (url_index % 50 == 0) or (url_index == num_urls):
            thread_pages = pandas.DataFrame(results)
            thread_pages.to_pickle('../data/output/thread_pages_{}.pkl'.format(url_index))
            results = list()

    return None


def parse_threads():
    # Reference variables
    results = list()

    # List out all serialized thread files
    thread_scrape_chunk_paths = sorted(glob.glob('../data/output/thread_pages_*.pkl'))
    logging.info(
        'List of scrape chunks to parse: {}, {}'.format(len(thread_scrape_chunk_paths), thread_scrape_chunk_paths))

    # Iterate through serialized thread files
    for chunk_index, thread_scrape_chunk_path in enumerate(thread_scrape_chunk_paths):
        logging.info('Working to parse thread_scrape_chunk_path: {}, {}'.format(chunk_index, thread_scrape_chunk_path))

        chunk = pandas.read_pickle(thread_scrape_chunk_path)

        # Iterate through individual thread pages
        for thread_index, thread_page in chunk.iterrows():
            logging.info('Working thread_page: {} from {}, with url: {}'.
                          format(thread_index, thread_scrape_chunk_path, thread_page['page_url']))

            page_html = thread_page['page_html']
            soup = BeautifulSoup(str(page_html))

            # Iterate through posts on thread_page
            for post in soup.find_all(class_='tpost'):

                result_dict = dict()
                result_dict.update(thread_page)
                del result_dict['page_html']

                try:
                    # Pull permalink
                    for link in soup.find_all('a'):
                        element_name = link.get('id')
                        if element_name is not None and str(element_name).startswith('postcount'):
                            result_dict['permalink'] = link.attrs['href']
                            result_dict['post_id'] = link.get_text()
                            logging.debug('Working post: {}'.format(result_dict['permalink']))


                    result_dict['username'] = post.find(class_='bigusername').get_text()

                    result_dict['user_info'] = post.find(class_='tcell alt2').get_text()
                    result_dict['text'] = post.find(class_='tcell alt1').get_text()

                    # Search through all of the links for the timestamp link
                    for link in soup.find_all('a'):
                        element_name = link.get('name')

                        # If the link describes a timestamp, add that timestampe
                        if element_name is not None and str(element_name).startswith('post'):
                            result_dict['timestamp'] = str(link.next_sibling)
                    result_dict['error'] = False
                except:
                    logging.warning('Issue!')
                    result_dict['error'] = True

                    # Add thread post to results
                    results.append(result_dict)


        chunk_posts = pandas.DataFrame(results)
        chunk_posts.to_pickle('../data/output/thread_posts_{}.pkl'.format(chunk_index))
        results = list()
    return

def agg_results():
    thread_post_chunk_paths = sorted(glob.glob('../data/output/thread_posts_*.pkl'))
    logging.info(
        'List of thread post chunks to parse: {}, {}'.format(len(thread_post_chunk_paths), thread_post_chunk_paths))

    results = list()

    for chunk_index, chunk_path in enumerate(thread_post_chunk_paths):

        chunk_df = pandas.read_pickle(chunk_path)

        results.append(chunk_df)

    observations = pandas.concat(results)

    observations.to_pickle('../data/output/posts.pkl')



# Main section
if __name__ == '__main__':
    main()
