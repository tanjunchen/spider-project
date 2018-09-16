import requests
from lxml import etree


def main():
    res = requests.get("http://www.qiushibaike.com/textnew/page/6/?s=5121955")
    html = res.text
    html = etree.HTML(html)
    node_list = html.xpath('//div[@class="content"]//span//text()')
    print(node_list)
    for node in node_list:
        comments = node
        items = {
            "comments": comments
        }
    print(items)

if __name__ == '__main__':
    main()