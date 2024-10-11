# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 10:47:25 2024

@author: Xintang Zheng

"""
# %% imports
import base64
import hashlib
import hmac
import time
import urllib.parse
import requests
import json
import pandas as pd


# %%
class DingReporter:
    
    def __init__(self, config):
        self._url = config['url']
        self._secret = config['secret']

    def _sign(self):
        # 当前时刻
        timestamp = str(round(time.time() * 1000))
        # 获取 sign
        secret_enc = self._secret.encode('utf-8')
        string_to_sign_enc = '{}\n{}'.format(timestamp, self._secret).encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        # print(timestamp, secret_enc, string_to_sign_enc, hmac_code, sign)
        return timestamp, sign

    def send_text(self, text, msg_type='info'):
        # 定义不同类型的颜色
        if msg_type == 'success':
            color = 'green'
        elif msg_type == 'warning':
            color = 'orange'
        elif msg_type == 'error':
            color = 'red'
        else:
            color = 'black'
        
        # 使用HTML标签设置文本颜色
        formatted_text = f"<font color=\"{color}\">{text}</font>"
    
        # 定义Markdown消息
        headers = {'content-type': 'application/json'}
        data = {
            'msgtype': 'markdown',
            'markdown': {
                'title': text, 
                'text': formatted_text
            }
        }
        ts, signature = self._sign()
        try:
            # 发送POST请求
            requests.post('{}&timestamp={}&sign={}'.format(self._url, ts, signature),
                          data=json.dumps(data), headers=headers)
        except Exception as e:
            print(e)
            print('retry later')
            
    def send_markdown(self, title, markdown_text, msg_type='info'):
        # 定义不同类型的标题颜色
        if msg_type == 'success':
            color = 'green'
        elif msg_type == 'warning':
            color = 'orange'
        elif msg_type == 'error':
            color = 'red'
        else:
            color = 'black'
        
        # 通过HTML标签设置标题的颜色，并加粗
        formatted_title = f"<font color=\"{color}\"><b>{title}</b></font>"
        
        # 定义Markdown消息类型
        headers = {'content-type': 'application/json'}
        # 将 formatted_title 添加到 markdown_text 的内容中
        data = {
            'msgtype': 'markdown',
            'markdown': {
                'title': title,
                'text': f"{formatted_title}\n\n{markdown_text}"  # 将加粗和带颜色的标题添加到 Markdown 内容中
            }
        }
        
        ts, signature = self._sign()
        try:
            # 发送POST请求
            requests.post('{}&timestamp={}&sign={}'.format(self._url, ts, signature), 
                          data=json.dumps(data), headers=headers)
        except Exception as e:
            print(e)
            print('retry later')
            
            
# %%
def df_to_markdown(data, head=5, tail=5, show_all=False, columns=[]):
    # 如果是 Series，将其转换为带 index 的 DataFrame
    if isinstance(data, pd.Series):
        data = data.reset_index()  # 将 index 转换为列
        data.columns = columns  # 重新命名列名
    
    # 获取DataFrame的列名
    headers = list(data.columns)
    
    # 表头行
    header_line = '| ' + ' | '.join(headers) + ' |'
    
    # 分隔行
    separator_line = '| ' + ' | '.join(['---'] * len(headers)) + ' |'
    
    # 获取数据行
    data_lines = []
    
    if show_all:
        # 打印全部行
        for row in data.itertuples(index=False):
            data_lines.append('| ' + ' | '.join(map(str, row)) + ' |')
    else:
        # 获取前 head 行
        for row in data.head(head).itertuples(index=False):
            data_lines.append('| ' + ' | '.join(map(str, row)) + ' |')

        # 如果需要在头部和尾部之间加入省略行
        if len(data) > head + tail:
            data_lines.append('| ... | ' * len(headers))

        # 获取后 tail 行
        for row in data.tail(tail).itertuples(index=False):
            data_lines.append('| ' + ' | '.join(map(str, row)) + ' |')
    
    # 组合成Markdown表格
    markdown_text = '\n'.join([header_line, separator_line] + data_lines)
    
    return markdown_text
      
            
#%%
if __name__ == "__main__": 
    # 测试
    url = "https://oapi.dingtalk.com/robot/send?access_token=01f43bcaaf341691f18632086edb1781cb9a7d0291a8d8c2bc2cdaacaf3ddce4"
    secret = "SEC26eb268f718b9419b7f306676aa6658c0cc114b771c9c173980984edbae24db6"
    
    config = {'url': url, 'secret': secret}
    d = DingReporter(config)
    d.send_text('test')
     
