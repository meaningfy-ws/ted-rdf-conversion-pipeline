import requests


def get_notice_by_id(notice_id):
    url = f'https://ted.europa.eu/en/notice/{notice_id}/xml'
    headers = {'Accept': 'application/xml'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.text
    else:
        response.raise_for_status()


def get_one():
    # return get_notice_by_id('83554-2024')
    with open('example.xml', 'r') as file:
        return file.read()

# def get_some_notice_ids(how_many, base_id=83554, year=2024):
#     return [f"{base_id + index}-{year}" for index in range(how_many)]
#
# notice_ids = get_some_notice_ids(2)
# for notice_id in notice_ids:
#     xml_string = get_notice_by_id(notice_id)
#     print(f'*************** {notice_id}')
#     print(xml_string)
