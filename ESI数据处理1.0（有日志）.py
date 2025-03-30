import pandas as pd
import re
from sqlalchemy import create_engine
from typing import List, Tuple, Optional
import time
from datetime import datetime
from sqlalchemy.sql import text

class ESIMatcher:
    def __init__(self, db_config: dict):
        self.engine = create_engine(f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 数据库连接成功")
        
    def _process_cited_table(self, cited_table: str) -> pd.DataFrame:
        """处理被引文献表（新增AU字段）"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始处理被引表 {cited_table}")
        t_start = time.time()
        
        query = f"SELECT UT, DI, J9, VL, PY, BP, AU FROM `{cited_table}`"
        df = pd.read_sql(query, self.engine)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 读取被引表完成，共 {len(df)} 条记录，耗时 {time.time()-t_start:.2f}s")

        # 提取第一个作者并标准化格式
        df['first_AU'] = (
            df['AU']
            .astype(str)
            .str.split('###')
            .str[0]  # 取第一个作者
            .str.replace(r'[\s,]+', '', regex=True)  # 去除空格和逗号
            .str.lower()  # 统一小写
        )
        # 数据清洗（仅要求PY/J9/VL非空）
        df = df.dropna(subset=['PY', 'J9', 'VL'])
        df['VL'] = df['VL'].astype(str).str.replace(r'\D', '', regex=True)
        df['BP'] = df['BP'].astype(str).str.replace(r'\D', '', regex=True).replace('', pd.NA)
        valid_count = len(df)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 数据清洗完成，有效记录 {valid_count:,} 条")

        # 修改new_id生成逻辑
        def generate_new_id(row):
            base = f"{row['PY']},{row['J9']},V{row['VL']}"
            if pd.notna(row['BP']):
                return f"{base},P{row['BP']}".replace(" ", "")
            else:
                return f"{row['first_AU']},{base}".replace(" ", "")  # BP为空时使用AU补充
            
        df['new_id'] = df.apply(generate_new_id, axis=1)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] new_id生成完成（支持AU补充）")
        
        return df[['UT', 'DI', 'new_id']].copy()

    def _process_citing_table(self, citing_table: str) -> pd.DataFrame:
        """处理施引文献表（添加日志）"""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 开始处理施引表 {citing_table}")
        t_start = time.time()
        
        query = f"SELECT UT, CR FROM `{citing_table}`"
        df = pd.read_sql(query, self.engine)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 读取施引表完成，共 {len(df)} 条记录，耗时 {time.time()-t_start:.2f}s")

        # 拆分参考文献
        t_split = time.time()
        # 保证CR字段是字符串
        df['CR'] = df['CR'].astype(str)
        # 清洗 CR 列
        df['CR'] = (
            df['CR']
            .str.strip()  # 去除首尾空格（可选）
            # .str.replace(r'\s+', '', regex=True)  # 去除所有内部空格
            .str.split('###')
            .apply(lambda x: [c.strip() for c in x if c.strip()])
        )
        df = df.explode('CR').dropna(subset=['CR']).copy()
        # 新增DOI提取功能
        doi_pattern = r'\b(?:doi|DOI)[:\s]\s*(10\.\d{4,}/\S+)'  # 匹配 DOI: 10.xxx 或 doi 10.xxx 格式
        df['CR-DI'] = df['CR'].str.extract(doi_pattern, expand=False)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] CR字段拆分完成，膨胀至 {len(df)} 条，耗时 {time.time()-t_split:.2f}s")
        return df

    def _new_id_match(self, cited_df: pd.DataFrame, citing_df: pd.DataFrame) -> pd.DataFrame:
        """直接硬匹配实现"""
        if cited_df.empty or citing_df.empty:
            return pd.DataFrame()
        # --------------------------
        # 0330-修改：AU字段辅助（辅助AI版）
        # --------------------------
        # 预先生成小写映射字典（key:小写new_id，value:原始new_id）
        new_id_map = {
            nid.lower(): nid  # 例如："lucaa,2019,farmacia,v67" -> "LucaA,2019,FARMACIA,V67"
            for nid in cited_df['new_id'].unique().tolist()
        }
        print(f"[DEBUG] 预生成小写new_id映射，数量：{len(new_id_map)}")

        def contains_new_id(cr_text):
            # 临时处理CR文本：去空格 + 全小写
            cr_str = str(cr_text).replace(" ", "").lower()
            # 遍历所有小写new_id进行子串匹配
            for nid_lower, original_nid in new_id_map.items():
                if nid_lower in cr_str:
                    return original_nid  # 返回原始大小写的new_id
            return None

        # 应用匹配函数
        t_start = time.time()
        citing_df['matched_new_id'] = citing_df['CR'].apply(contains_new_id)
        matches = citing_df.dropna(subset=['matched_new_id'])
        print(f"[DEBUG] 匹配耗时：{time.time()-t_start:.2f}s，命中{len(matches)}条")

        # 合并结果（使用原始new_id确保准确合并）
        merged = matches.merge(
            cited_df,
            left_on='matched_new_id',
            right_on='new_id'
        )[['UT_y', 'UT_x']].rename(
            columns={'UT_y': 'cited_UT', 'UT_x': 'citing_UT'}
        ).drop_duplicates()
        # 测试：保存中间结果用于比对
        merged.to_csv(f"new_id_final_matches_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv", index=False)
        print(f"成功保存中间结果！")
        return merged

    def _doi_match(self, cited_df: pd.DataFrame, citing_df: pd.DataFrame) -> pd.DataFrame:
        """DOI匹配实现"""
        if cited_df.empty or citing_df.empty:
            return pd.DataFrame()
            
        dois = cited_df['DI'].dropna().unique()
        pattern = r'\b(?:doi\.org/)?(?:' + '|'.join(map(re.escape, dois)) + r')\b'
        matches = citing_df[citing_df['CR'].str.contains(pattern, na=False, regex=True)].copy()
        # 将matches和cited_df进行合并，合并的键是'DI'列
        merged = matches.merge(cited_df, left_on ='CR-DI',right_on='DI')[['UT_y', 'UT_x']].rename(
            columns={'UT_y': 'cited_UT', 'UT_x': 'citing_UT'}
        ).drop_duplicates()
        # 测试：保存中间结果用于比对
        merged.to_csv(f"doi_final_matches_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv", index=False)
        print(f"成功保存中间结果！")
        return merged

    def _match_pair(self, cited_table: str, citing_table: str) -> Optional[pd.DataFrame]:
        """表处理与匹配执行"""
        print(f"\n{'='*50}\n[{datetime.now().strftime('%H:%M:%S')}] 开始匹配 {cited_table} -> {citing_table}")
        try:
            cited_df = self._process_cited_table(cited_table)
            citing_df = self._process_citing_table(citing_table)
        except Exception as e:
            print(f"[ERROR] 表处理失败: {str(e)}")
            return None

        # 双模式匹配
        t_match = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始NewID匹配...")
        new_id_results = self._new_id_match(cited_df, citing_df)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] NewID匹配完成，匹配到 {len(new_id_results)} 条")
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始DOI匹配...")
        doi_results = self._doi_match(cited_df, citing_df)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] DOI匹配完成，匹配到 {len(doi_results)} 条")
        
        merged = pd.concat([new_id_results, doi_results]).drop_duplicates()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 合并去重完成，总匹配 {len(merged)} 条，总耗时 {time.time()-t_match:.2f}s")
        return merged

    def batch_match(self, citing_tables: List[str], cited_tables: List[str], result_table_format: str = "{citing_table}_cite_{cited_table}") -> dict:
        """入口函数"""
        print(f"\n{'#'*50}\n[{datetime.now().strftime('%H:%M:%S')}] 启动批量匹配任务")
        print(f"施引表列表: {citing_tables}")
        print(f"被引表列表: {cited_tables}")
        results = {}
        
        for citing_table in citing_tables:
            print(f"\n{'='*30}\n正在处理施引表: {citing_table}")
            
            # 从表名提取年份（假设表名前4位为年份）
            year = citing_table[:4] if citing_table[:4].isdigit() else None
            if not year:
                print(f"[WARNING] 表名 {citing_table} 无法提取年份，跳过被引频次计算")
            
            for cited_table in cited_tables:
                print(f"\n匹配被引表: {cited_table}")
                
                merged = self._match_pair(cited_table, citing_table)
                if merged is None or merged.empty:
                    results[(citing_table, cited_table)] = "匹配失败或无结果"
                    continue
                
                # 生成结果表名并保存
                result_table = result_table_format.format(
                    citing_table=citing_table,
                    cited_table=cited_table
                )
                
                try:
                    merged.to_sql(
                        name=result_table,
                        con=self.engine,
                        if_exists='replace',
                        index=False
                    )
                    results[(citing_table, cited_table)] = f"成功写入表: {result_table}"
                except Exception as e:
                    results[(citing_table, cited_table)] = f"写入失败: {str(e)}"
                    continue
                
                # ---------- 新增被引频次统计逻辑 ----------
                if year:
                    try:
                        # 统计当前匹配结果中的被引频次
                        citation_counts = merged['cited_UT'].value_counts().reset_index()
                        citation_counts.columns = ['UT', 'count']
                        
                        # 使用原生SQL高效更新被引表
                        with self.engine.begin() as conn:  # 自动提交事务
                            # 检查并添加年份字段
                            column_name = f"citation_count_{year}"
                            if not conn.execute(
                                text(f"SHOW COLUMNS FROM `{cited_table}` LIKE :column"),
                                {'column': column_name}
                            ).fetchone():
                                conn.execute(text(f"ALTER TABLE `{cited_table}` ADD COLUMN `{column_name}` INT DEFAULT 0"))
                            
                            # 批量更新被引次数
                            update_stmt = text(f"""
                                UPDATE `{cited_table}` 
                                SET `{column_name}` = `{column_name}` + :count 
                                WHERE `UT` = :ut
                            """)
                            conn.execute(update_stmt, [{'ut': row['UT'], 'count': row['count']} for _, row in citation_counts.iterrows()])
                            
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] 成功更新被引表 {cited_table} 的{column_name}字段")
                        
                    except Exception as e:
                        print(f"[ERROR] 被引频次统计失败: {str(e)}")
        
        return results


if __name__ == "__main__":
    # 配置数据库连接
    db_config = {
        'username': 'root',
        'password': 'luyi123456',
        'host': 'localhost',
        'database': 'wos_data'
    }
    
    # 初始化匹配器
    matcher = ESIMatcher(db_config)
    
    # 自定义匹配组合
    match_results = matcher.batch_match(
        citing_tables=['2023esi_Physics_of_Fluids'],  # 施引表列表 # 2023esi_farmacia/2023esi_Physics_of_Fluids
        cited_tables=['2023esi_Physics_of_Fluids'],   # 被引表列表
        result_table_format="{citing_table}_citing_{cited_table}"  # 自定义结果表名
    )
    
    # 输出结果状态
    print("匹配结果汇总:")
    for pair, status in match_results.items():
        print(f"{pair[0]} 引用 {pair[1]}: {status}")