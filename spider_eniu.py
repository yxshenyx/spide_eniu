import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json

class FinancialSpider:
    def __init__(self, stock_code):
        self.stock_code = stock_code
        self.urls = [
            f'https://eniu.com/table/cwzba/{stock_code}/q/0',  # 财务报表
            f'https://eniu.com/table/fzba/{stock_code}/q/0',   # 资产负债表
            f'https://eniu.com/table/llba/{stock_code}/q/0/q/all'  # 现金流量表
        ]
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        }
        self.keep_indicators = [
            '营业收入', '营业总成本','营业利润', '扣非净利润', '毛利率', '净利率',
            '净资产收益率(ROE)', '资产总计', '流动资产合计', '固定资产', '在建工程','负债总计',
             '货币资金', '应收账款',
            '应收票据', '存货', 
            '流动负债合计', '非流动负债合计', '负债合计','资产负债率',
            '期末现金及现金等价物余额'
        ]

    def fetch_data(self):
        """获取所有数据并合并"""
        all_dfs = []
        
        for url in self.urls:
            try:
                response = requests.get(url=url, headers=self.headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'lxml', from_encoding='utf-8')
                content = soup.body.p.text
                
                # 将字符串转换为Python字典
                data_dict = json.loads(content)
                
                # 将字典转换为DataFrame并只保留前25列
                df = pd.DataFrame(data_dict).iloc[:, :25]
                
                # 将DataFrame添加到列表中
                all_dfs.append(df)
                
                # 添加延时，避免请求过快
                time.sleep(1)
                
            except Exception as e:
                print(f"Error fetching data from {url}: {str(e)}")
                continue
        
        return all_dfs

    def process_data(self, all_dfs):
        """处理和清理数据"""
        # 合并所有DataFrame
        combined_df = pd.concat(all_dfs, axis=0, ignore_index=True)
        
        # 清理所有列中的尖括号
        for column in combined_df.columns:
            combined_df[column] = combined_df[column].astype(str)
            combined_df[column] = combined_df[column].str.replace(r'<[^>]+>', '', regex=True)
        
        # 筛选指定的指标
        df_filtered = combined_df[combined_df['keyName'].isin(self.keep_indicators)]
        
        # 删除重复行
        df_filtered = df_filtered.drop_duplicates(subset=['keyName'])
        
        return df_filtered

    def run(self):
        """运行爬虫并保存数据"""
        # 获取数据
        all_dfs = self.fetch_data()
        
        if not all_dfs:
            print("No data fetched")
            return None
        
        # 处理数据
        combined_df = pd.concat(all_dfs, axis=0, ignore_index=True)
        
        # 清理所有列中的尖括号
        for column in combined_df.columns:
            combined_df[column] = combined_df[column].astype(str)
            combined_df[column] = combined_df[column].str.replace(r'<[^>]+>', '', regex=True)
        
        # 保存完整表格
        full_output_filename = f'FS_{self.stock_code}_full.csv'
        combined_df.to_csv(full_output_filename, encoding='utf-8', index=False)
        print(f"Full data saved to {full_output_filename}")
        
        # 筛选指定的指标
        df_filtered = combined_df[combined_df['keyName'].isin(self.keep_indicators)]
        
        # 删除重复行
        df_filtered = df_filtered.drop_duplicates(subset=['keyName'])
        
        # 保存筛选后的表格
        # filtered_output_filename = f'FS_{self.stock_code}_filtered.csv'
        # df_filtered.to_csv(filtered_output_filename, encoding='utf-8', index=False)
        # print(f"Filtered data saved to {filtered_output_filename}")
        
        print("\nFiltered DataFrame:")
        print(df_filtered)
        return df_filtered, combined_df

def main():
    # 使用示例
    stock_code = 'sz300001'
    spider = FinancialSpider(stock_code)
    df, combined_df = spider.run()
    if df is not None:
        print("\nFetched Data:")
        print(df)

if __name__ == "__main__":
    main()
