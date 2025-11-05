import json
import logging
import re
import xml.etree.ElementTree as ET
from types import SimpleNamespace

logger = logging.getLogger(__name__)

# 解析XML内容
def xml_to_json(xml_string, as_string=False):
    try:
        # 处理XML声明
        if xml_string.startswith('<?xml'):
            xml_string = xml_string.split('?>', 1)[1]
            
        # 解析 XML 字符串
        root = ET.fromstring(xml_string)
        
        # 递归函数，将 XML 元素转换为字典
        def element_to_dict(element):
            result = {}
            
            # 添加属性
            if element.attrib:
                for key, value in element.attrib.items():
                    result[key] = value
            
            # 处理子元素
            children = list(element)
            if children:
                for child in children:
                    child_name = child.tag
                    child_dict = element_to_dict(child)
                    
                    # 如果同名子元素已存在，则转为列表
                    if child_name in result:
                        if not isinstance(result[child_name], list):
                            result[child_name] = [result[child_name]]
                        result[child_name].append(child_dict)
                    else:
                        result[child_name] = child_dict
            
            # 添加文本内容（如果有且没有其他属性或子元素）
            text = element.text
            if text and text.strip():
                if not result:  # 如果没有其他属性或子元素
                    return text.strip()
                result["_text"] = text.strip()
                
            return result
        
        # 转换为字典
        json_data = {root.tag: element_to_dict(root)}
        
        # 根据参数决定返回JSON字符串还是Python字典
        if as_string:
            return json.dumps(json_data, ensure_ascii=False, indent=2)
        else:
            return json_data
        
    except Exception as e:
        print(f"解析 XML 时出错: {e}")
        return None
    
def xml_to_obj(xml_string):
    try:
        # 处理XML声明
        if xml_string.startswith('<?xml'):
            xml_string = xml_string.split('?>', 1)[1]
            
        # 解析 XML 字符串
        root = ET.fromstring(xml_string)
        
        # 递归函数，将 XML 元素转换为字典
        def element_to_dict(element):
            result = {}
            
            # 添加属性
            if element.attrib:
                for key, value in element.attrib.items():
                    result[key] = value
            
            # 处理子元素
            children = list(element)
            if children:
                for child in children:
                    child_name = child.tag
                    child_dict = element_to_dict(child)
                    
                    # 如果同名子元素已存在，则转为列表
                    if child_name in result:
                        if not isinstance(result[child_name], list):
                            result[child_name] = [result[child_name]]
                        result[child_name].append(child_dict)
                    else:
                        result[child_name] = child_dict
            
            # 添加文本内容（如果有且没有其他属性或子元素）
            text = element.text
            if text and text.strip():
                if not result:  # 如果没有其他属性或子元素
                    return text.strip()
                result["_text"] = text.strip()
                
            return result
        
        # 转换为字典
        json_data = {root.tag: element_to_dict(root)}
        
        # 将字典转换为对象，以便使用点表示法访问
        def dict_to_obj(d):
            if isinstance(d, dict):
                # 创建SimpleNamespace对象
                obj = SimpleNamespace()
                for key, value in d.items():
                    # 处理Python关键字作为属性名的情况
                    if key in ['from', 'class', 'import', 'global', 'return', 'try', 'except', 'finally', 'raise', 'def', 'if', 'else', 'elif', 'for', 'while', 'in', 'is', 'not', 'and', 'or', 'lambda', 'with', 'as', 'assert', 'break', 'continue', 'del', 'exec', 'pass', 'print', 'yield']:
                        key = key + '_'
                    # 处理包含特殊字符或以数字开头的属性名
                    if not key.isalnum() or key[0].isdigit() or '-' in key:
                        key = 'attr_' + key
                    # 递归处理嵌套的字典和列表
                    setattr(obj, key, dict_to_obj(value))
                return obj
            elif isinstance(d, list):
                # 处理列表中的每个元素
                return [dict_to_obj(item) for item in d]
            else:
                # 基本类型直接返回
                return d
        
        # 转换整个字典为对象
        obj = dict_to_obj(json_data)
        return obj
        
    except Exception as e:
        print(f"解析 XML 时出错: {e}")
        return None

# 提取公众号文章
def extract_url_items(json_dict):
    result = ""

    def process_text_field(field):
        """处理可能是字典或字符串的文本字段"""
        if isinstance(field, dict) and "_text" in field:
            return field["_text"]
        return str(field) if field else ""
    
    def format_item(title, url, summary):
        """格式化单个项目"""
        # 处理字典类型
        title = process_text_field(title)
        url = process_text_field(url)
        summary = process_text_field(summary)

        # 纯文字分享需要删除文本中的超链接代码
        pattern = r'<a[^>]*>(.*?)</a>'
        match = re.search(pattern, title)
        if match:
            title = match.group(1)
        
        # HTML转义
        title = escape_html_chars(title)
        url = escape_html_chars(url)
        summary = escape_html_chars(summary)
        
        # 格式化summary
        if summary:
            summary = f"<blockquote>{summary}</blockquote>"
        
        return f'<a href="{url}">{title}</a>\n{summary}\n'
    
    try:
        # 首先检查是否有appmsg元素
        if "msg" in json_dict and "appmsg" in json_dict["msg"]:
            appmsg = json_dict["msg"]["appmsg"]
            
            # 获取主封面
            main_cover_url = appmsg.get('thumburl', '')

            # 检查是否有mmreader和item列表
            if "mmreader" in appmsg:
                mmreader = appmsg["mmreader"]
                
                # 检查是否有category和item列表
                if "category" in mmreader and "item" in mmreader["category"]:
                    items = mmreader["category"]["item"]
                    # 确保items是列表
                    if not isinstance(items, list):
                        items = [items]
                        
                    # 遍历item列表提取每篇文章的标题和URL
                    for item in items:
                        if "title" in item and "url" in item:
                            title = item["title"]
                            url = item["url"]
                            summary = ""
                            
                            # 尝试获取summary
                            if "summary" in item and item["summary"]:
                                summary = item["summary"]
                            # 如果没有summary，尝试从template_detail获取
                            elif "template_detail" in mmreader:
                                summary = extract_line_content(mmreader["template_detail"])
                            
                            result += format_item(title, url, summary)
            
            # 如果没有找到items，使用主文章信息
            if not result and "title" in appmsg and "url" in appmsg:
                title = appmsg["title"]
                url = appmsg["url"]
                summary = appmsg.get("des", "")
                result += format_item(title, url, summary)
    
    except Exception as e:
        print(f"提取标题和URL时出错: {e}")
    
    return result, main_cover_url

def extract_line_content(template_detail):
    """从template_detail中提取line_content"""
    summary = ""
    try:
        if "line_content" in template_detail and "lines" in template_detail["line_content"]:
            line_content = template_detail["line_content"]
            if "line" in line_content["lines"]:
                line_items = line_content["lines"]["line"]
                if not isinstance(line_items, list):
                    line_items = [line_items]
                
                line_texts = []
                for line_item in line_items:
                    if "key" in line_item and "value" in line_item:
                        key_word = get_text_from_field(line_item["key"])
                        value_word = get_text_from_field(line_item["value"])
                        
                        # 添加冒号
                        if key_word and not key_word.endswith((":", "：")):
                            key_word = key_word + ": "
                        
                        line_texts.append(f"{key_word}{value_word}")
                
                if line_texts:
                    summary = "\n".join(line_texts)
    except Exception as e:
        print(f"提取line_content时出错: {e}")
    
    return summary

def get_text_from_field(field):
    """从字段中获取文本内容"""
    if isinstance(field, dict):
        if "word" in field:
            word = field["word"]
            if isinstance(word, dict) and "_text" in word:
                return word["_text"]
            return str(word) if word else ""
        elif "_text" in field:
            return field["_text"]
    return str(field) if field else ""

# 字符串添加转义符匹配TG的markdown输出
def escape_markdown_chars(text):
    """
    检测字符串中是否包含 Markdown 特殊字符，并为它们添加转义符 \
    
    参数:
        text (str): 需要处理的字符串
        
    返回:
        str: 处理后的字符串，特殊字符前添加了转义符 \
    """
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    result = ""
    
    for char in text:
        if char in special_chars:
            result += "\\" + char
        else:
            result += char
            
    return result

def escape_html_chars(text):
    """
    智能转义HTML特殊字符，专门针对Telegram Bot API
    保留Telegram支持的HTML标签，转义其他特殊字符
    """
    if not isinstance(text, str):
        return str(text)
    
    # Telegram Bot API 支持的HTML标签模式
    telegram_html_patterns = [
        # 基础格式标签
        r'<b>.*?</b>',
        r'<strong>.*?</strong>',
        r'<i>.*?</i>',
        r'<em>.*?</em>',
        r'<u>.*?</u>',
        r'<s>.*?</s>',
        r'<strike>.*?</strike>',
        r'<del>.*?</del>',
        
        # 剧透标签
        r'<span\s+class=["\']tg-spoiler["\']>.*?</span>',
        
        # 链接标签
        r'<a\s+href=["\'][^"\']*["\'](?:\s+[^>]*)?>.*?</a>',
        
        # 代码标签
        r'<code>.*?</code>',
        r'<pre>.*?</pre>',
        r'<pre><code(?:\s+class=["\']language-[^"\']*["\'])?>.*?</code></pre>',
        
        # 引用块
        r'<blockquote(?:\s+expandable)?(?:\s+[^>]*)?>.*?</blockquote>',
        
        # 自定义emoji
        r'<tg-emoji\s+emoji-id=["\'][^"\']*["\']>.*?</tg-emoji>',
    ]
    
    # 找到所有有效HTML标签的位置
    html_ranges = []
    for pattern in telegram_html_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE | re.DOTALL):
            html_ranges.append((match.start(), match.end()))
    
    # 如果没有HTML标签，直接转义所有特殊字符
    if not html_ranges:
        return escape_special_chars(text)
    
    # 合并重叠的HTML标签范围
    html_ranges.sort()
    merged_ranges = []
    for start, end in html_ranges:
        if merged_ranges and start <= merged_ranges[-1][1]:
            merged_ranges[-1] = (merged_ranges[-1][0], max(merged_ranges[-1][1], end))
        else:
            merged_ranges.append((start, end))
    
    # 分段处理：HTML标签内不转义，标签外转义
    result = []
    last_end = 0
    
    for start, end in merged_ranges:
        # 转义HTML标签前的普通文本
        before_html = text[last_end:start]
        if before_html:
            result.append(escape_special_chars(before_html))
        
        # 保留HTML标签原样
        html_tag = text[start:end]
        result.append(html_tag)
        last_end = end
    
    # 转义最后一段普通文本
    after_html = text[last_end:]
    if after_html:
        result.append(escape_special_chars(after_html))
    
    return ''.join(result)


def escape_special_chars(text):
    """
    转义HTML特殊字符
    注意顺序：必须先转义 & 符号
    """
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    return text
