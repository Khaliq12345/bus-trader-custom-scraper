import pandas as pd
from botasaurus.request import request, Request
from selectolax.parser import HTMLParser
from sqlalchemy import create_engine
import sys
from pathlib import Path
import config

for x in sys.argv:
    if '--output' in x:
        file_name = x.replace('--output=', '').strip()
        break
    else:
        file_name = None


class Manager:
    bus_items = []
    file_name = file_name

manager = Manager()

def get_text_content(soup: HTMLParser, selector: str, direct: bool = False, look_for: str = ''):
    if direct:
        content_html = soup.css_first(selector)
        if content_html:
            content_html = content_html.text(strip=True)
            content_html = content_html.replace('Price: $', '')
            return content_html
    else:
        looks = soup.css(selector)
        for look in looks:
            if look_for in look.text():
                return look.text(strip=True).replace(look_for, '')

def get_images(soup: HTMLParser, selector):
    image_htmls = soup.css(selector)
    images = [x.attributes['src'] for x in image_htmls]
    images = '; '.join(images)
    return images

@request(output=None, max_retry=5, retry_wait=5)
def get_detail_page(request: Request, url):
    response = request.get(url)
    if response.status_code == 200:
        soup = HTMLParser(response.text)
        bus_item = {
            'title': get_text_content(soup, 'h1.title', direct=True),
            'price': get_text_content(soup, '.price', direct=True),
            'type': get_text_content(soup, '.detail-list', look_for='Type:'),
            'mileage': get_text_content(soup, '.detail-list', look_for='Mileage:'),
            'passengers': get_text_content(soup, '.detail-list', look_for='Passengers:'),
            'history': get_text_content(soup, '.detail-list', look_for='History:'),
            'location': get_text_content(soup, '.detail-list', look_for='Location:'),
            'description': get_text_content(soup, '#description', direct=True),
            'images': get_images(soup, 'img.bus-img')
        }
        manager.bus_items.append(bus_item)
    else:
        raise ConnectionError()
    
@request(output=None)
def main(request: Request, data):
    page_num = 1
    while True:
        print(f'Page {page_num}')
        response = request.get(f'https://www.bustrader.com/buses/page/{page_num}/')
        print(f"Response url: {response.url}")
        if response.status_code == 200:
            soup = HTMLParser(response.text)
            buses = soup.css('a.listing')
            if len(buses) > 0:
                bus_links = [bus.attributes['href'] for bus in buses]
                get_detail_page(bus_links)
            else:
                break
        else:
            break
        page_num += 1
        
def save_data_based_on_formats(file_name):
    output_folder = Path('outputs')
    output_folder.mkdir(exist_ok=True)
    file_name = f'outputs/{file_name}'
    df = pd.DataFrame(manager.bus_items)
    if '.xlsx' in file_name:
        df.to_excel(file_name, index=False)
    elif '.json' in file_name:
        df.to_json(file_name, orient='records', index=False)
    elif '.csv' in file_name:
        df.to_csv(file_name, index=False)
    else:
        print(f"File Format is not supported, use one of (.xlsx, .json, .csv)")
        
def save_to_database(table_name):
    df = pd.DataFrame(manager.bus_items)
    connection_string = config.connection_string
    engine = create_engine(connection_string)
    with engine.connect() as con:
        df.to_sql(table_name, con, if_exists='replace', index=False)

def save_data():
    if manager.file_name:
        save_data_based_on_formats(file_name)
    save_to_database('buses')
    
    
if __name__ == '__main__':
    print(manager.file_name)
    main()
    save_data()