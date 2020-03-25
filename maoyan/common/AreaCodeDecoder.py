import json
import os
import jieba
import requests


class AreaCodeDecoder(object):
    current_path = os.path.dirname(__file__)

    def __init__(self, del_file="jieba_del.txt", areacode_file="areacode.txt", area_json_file="china_city_area.json",
                 baidu_ak="eXiTVqhBbnU7TeF3WrtGAvxXkIUXBRwg"):
        self.cities = dict()
        with open(os.path.join(self.current_path, del_file), encoding="utf8") as file:
            for word in file:
                jieba.del_word(word)
        with open(os.path.join(self.current_path, areacode_file), encoding="utf8") as file:
            for line in file:
                city, areacode = line.replace("\r", "").replace("\n", "").split(",")
                self.cities[city] = areacode
        with open(os.path.join(self.current_path, area_json_file), encoding="utf8") as file:
            self.area = json.load(file)
        self.geocoder_url_template = "http://api.map.baidu.com/geocoder/v2/?address={addr}&output=json&ak=" + baidu_ak
        self.city_url_template = "http://api.map.baidu.com/geocoder/v2/?ak=" + baidu_ak \
                                 + "&location={lat},{lng}&output=json"
        self.session = requests.session()

    def sentence_to_areacode(self, sentence, seq=""):
        if seq:
            tokens = sentence.split(seq)
        else:
            tokens = list(jieba.cut(sentence))
        areas = []
        for t in tokens:
            if t in self.cities:
                areas.append(t)
        area_code = ""
        for area in areas:
            area_code = self.cities[area] if self.cities[area] > area_code else area_code
        city = ",".join(areas)
        item_short_name = seq.join([t for t in tokens if t not in areas])
        return city, item_short_name, area_code

    def name_to_areacode(self, name):
        area_code = self.cities.get(name, None)
        if area_code is None:
            area_name = self._guess(name, [n for n in self.cities])
            if area_name is not None:
                return self.cities[area_name]
            return
        return area_code

    def address_to_areacode(self, addr):
        res = self.session.get(self.geocoder_url_template.format(addr=addr))
        baidu = res.json()
        if baidu["status"] != 0:
            return
        lng = baidu["result"]["location"]["lng"]
        lat = baidu["result"]["location"]["lat"]
        addr = self._city_info(lat, lng)
        return self._format_addr_code(addr)

    def format_addr_code(self, format_addr, seq=","):
        tokens = format_addr.split(seq)
        prov = self.area["children"].get(tokens[0], None)
        if prov is None:
            prov = self._guess(tokens[0], [k for k in self.area["children"]])
        else:
            prov = prov["name"]
        if prov is None:
            return
        city = self.area["children"][prov].get(tokens[1], None)
        if city is None:
            city = self._guess(tokens[1], [k for k in self.area["children"][prov]["children"]])
        else:
            city = city["name"]
        if city is None:
            return self.area["children"][prov]["code"]
        district = self.area["children"][prov]["children"][city].get(tokens[2], None)
        if district is None:
            district = self._guess(tokens[2], [k for k in self.area["children"][prov]["children"][city]["children"]])
        else:
            district = district["name"]
        if district is None:
            return self.area["children"][prov]["children"][city]["code"]
        return self.area["children"][prov]["children"][city]["children"][district]["code"]

    def _city_info(self, lat, lng):
        res = self.session.get(self.city_url_template.format(lat=lat, lng=lng))
        baidu = res.json()
        if baidu["status"] != 0:
            return
        address_component = baidu["result"]["addressComponent"]
        return ",".join([address_component["province"], address_component["city"], address_component["district"]])

    @staticmethod
    def _guess(target, lis):
        def alg(l):
            score = AreaCodeDecoder._jaccard_distance(set(target), set(l))
            return 0 if target in l else 0 if l in target else score

        try:
            scores = map(alg, lis)
            maxx = sorted(enumerate(scores), key=lambda x: x[1])
            if maxx[0][1] <= 0.5:
                return lis[maxx[0][0]]
        except IndexError:
            return

    @staticmethod
    def _jaccard_distance(label1, label2):
        return (len(label1.union(label2)) - len(label1.intersection(label2))) / len(label1.union(label2))


if __name__ == '__main__':
    a = AreaCodeDecoder()
    print(a.name_to_areacode("湖南"))
