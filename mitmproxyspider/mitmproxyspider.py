#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mitmproxy import ctx, flow


def job(url):
    injected_javascript = '''
    // overwrite the `languages` property to use a custom getter
    Object.defineProperty(navigator, "languages", {
      get: function() {
        return ["zh-CN","zh","zh-TW","en-US","en"];
      }
    });
    // Overwrite the `plugins` property to use a custom getter.
    Object.defineProperty(navigator, 'plugins', {
      get: () => [1, 2, 3, 4, 5],
    });
    // Pass the Webdriver test
    Object.defineProperty(navigator, 'webdriver', {
      get: () => false,
    });
    // Pass the Chrome Test.
    // We can mock this in as much depth as we need for the test.
    window.navigator.chrome = {
      runtime: {},
      // etc.
    };
    // Pass the Permissions Test.
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
      parameters.name === 'notifications' ?
        Promise.resolve({ state: Notification.permission }) :
        originalQuery(parameters)
    );
    '''
    # Only process 200 responses of HTML content.
    if not flow.response.status_code == 200:
        return
    # Inject a script tag containing the JavaScript.
    html = flow.response.text
    html = html.replace('<head>', '<head><script>%s</script>' % injected_javascript)
    flow.response.text = str(html)
    ctx.log.info('>>>> js代码插入成功 <<<<')

    # 只要url链接以target开头，则将网页内容替换为目前网址
    if flow.url.startswith(url):
        flow.response.text = flow.url
        print(flow.url)
    print(url)


if __name__ == '__main__':
    target = 'https://passport.zcool.com.cn/regPhone.do?appId=1006&cback=https://my.zcool.com.cn/focus/activity'
    job(target)
