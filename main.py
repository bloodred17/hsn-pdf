import datetime
import json
import os
import uuid
import tabula as tb
from PyPDF2 import PdfReader
import re
import io

from rate import Rate

column_coordinates = [34, 102.24, 136.8, 428.4, 482.4, 531.36, 582.48, 634.32, 685.44, 753.84]
column_names = ["heading", "cd", "article", "statistical_unit", "general", "eu_uk", "efta", "sadc", "mercosur",
                "afcfta"]
heading_data_keys = ["article", "statistical_unit", "general", "eu_uk", "efta", "sadc", "mercosur", "afcfta"]
rate_keys = ["general", "eu_uk", "efta", "sadc", "mercosur", "afcfta"]
description_keys = ["main_head_description", "sub_head_1_description", "sub_head_2_description", "description"]


def get_hsn_type(hsn_number: str, article: str, cd: str):
    dashes = 0
    partitioned_hsn = hsn_number.split('.')
    if len(partitioned_hsn[0]) == 2:
        return "heading"

    if re.search(r"- - - (.*)", article):
        dashes = 3
    elif re.search(r"- - (.*)", article):
        dashes = 2
    elif re.search(r"- (.*)", article):
        dashes = 1

    if dashes == 1 and len(cd) == 0:
        return "sub_heading_1"
    elif dashes == 1 and len(cd) > 0:
        return "article"
    elif dashes == 2 and len(cd) == 0:
        return "sub_heading_2"
    elif dashes >= 2 and len(cd) > 0:
        return "article"


def get_column_name_quick(left_coordinate: float):
    col_coordinates = column_coordinates.copy()
    if left_coordinate < col_coordinates[0]:
        return None
    col_coordinates.append(left_coordinate)
    col_coordinates.sort()
    index = col_coordinates.index(left_coordinate)
    try:
        return column_names[index - 1]
    except:
        return None


def get_column_name(left_coordinate: float):
    if 34 <= left_coordinate < 102.24:
        return "heading"
    elif 102.24 <= left_coordinate < 136.8:
        return "cd"
    elif 136.8 <= left_coordinate < 428.4:
        return "article"
    elif 428.4 <= left_coordinate < 482.4:
        return "statistical_unit"
    elif 482.4 <= left_coordinate < 531.36:
        return "general"
    elif 531.36 <= left_coordinate < 582.48:
        return "eu_uk"
    elif 582.48 <= left_coordinate < 634.32:
        return "efta"
    elif 634.32 <= left_coordinate < 685.44:
        return "sadc"
    elif 685.44 <= left_coordinate < 753.84:
        return "mercosur"
    elif 753.84 <= left_coordinate:
        return "afcfta"
    return None


def process_raw_data(key: str, value: str):
    if key == "article":
        value = value.replace("-", "")
        value = value.strip()
    return value


def process_rate(text: str):
    text = text.strip()
    text = text.replace(",", "")
    if text == "free":
        return [
            Rate(0.0, "").get_dict()
        ]

    or_pattern = re.search(r"(\d+)([%\w\/]+)\sor\s(\d+)([%\w\/]+)", text)
    if or_pattern:
        val_1, unit_1, val_2, unit_2 = or_pattern.groups()
        return [
            Rate(float(val_1), unit_1).get_dict(),
            Rate(float(val_2), unit_2).get_dict(),
        ]

    percent_pattern = re.search(r"([\d,.]+)([%\w\/]+)", text)
    if percent_pattern:
        val_1, unit_1 = percent_pattern.groups()
        return [
            Rate(float(val_1), unit_1).get_dict()
        ]

    max_pattern = re.search(r"([\d,.]+)([%\w\/]+)(?:[\n\s]+)with(?:[\n\s]+)a(?:[\n\s]+)maximum(?:[\n\s]+)of(?:[\n\s]+)([\d,.]+)([%\w\/]+)", text)
    if max_pattern:
        val_1, unit_1, val_2, unit_2 = max_pattern.groups()
        rate = Rate(float(val_1), unit_1)
        rate.max = Rate(float(val_2), unit_2)
        return [
            rate.get_dict()
        ]

    return text


def process_descriptions(text: str):
    text = text.replace("-", "")
    text = text.replace(":", "")
    text = text.strip()
    return text


def release(filename: str, data: list):
    # Process before output
    for item in data:
        for key in item.keys():
            if key in rate_keys:
                item[key] = process_rate(item[key])
            if key in description_keys:
                item[key] = process_descriptions(item[key])

    folder = "./output"
    if not os.path.isdir(folder):
        os.makedirs(folder)
    filepath = folder + "/" + filename + '.json'
    with io.open(filepath, 'w', encoding='utf8') as outfile:
        str_ = json.dumps(data, indent=2, ensure_ascii=False)
        print("writing file to " + filepath)
        outfile.write(str_)
        outfile.close()


def collect_header_data_from_columns(value, to=None):
    if not to:
        to = value
    for key in value.keys():
        if key in heading_data_keys:
            if value[key] == to["article"]:
                continue
            to["article"] += value[key]


def collect_article_data(last_article, value):
    keys = value.keys()
    if value.get("article"):
        last_article["description"] += " " + value.get("article")
    for key in keys:
        if last_article.get(key):
            last_article[key] += " " + value.get(key)


def extract(name):
    file = "hsn.pdf"

    reader = PdfReader(file)
    pdf_page_count = len(reader.pages)
    print("Total Pages: " + str(pdf_page_count))

    date_data = tb.read_pdf(file, area=(18, 32, 35, 117), pages='1', stream=True,
                            output_format="json")[0]["data"][0][0]
    # print(json.dumps(date_data["text"], indent=2))

    year, month, day = re.search(r"Date:\s(\d{4})-(\d{2})-(\d{2})", date_data["text"]).groups()
    date = datetime.date(int(year), int(month), int(day))
    print(date)

    sections = []
    current_section = {}
    last_chapter_name = ""
    articles = []
    article_queue = []
    previous_article = {}
    current_heading = {}
    current_sub_heading_1 = {}
    current_sub_heading_2 = {}
    last_item_type: str

    for index in range(pdf_page_count):
        # if index < 2:
        if index < 13:
            continue
        if index == 32:
            break

        print("Page " + str(index))

        # Check if page is a section or chapter
        possible_section = tb.read_pdf(file, area=(57.6, 34.56, 92.16, 806.4), pages=str(index), stream=True,
                                       output_format="json")[0]["data"]
        mapped_section = list(map(lambda x: x[0]["text"], possible_section))
        found_section = re.match(r"(SECTION\s(\w+))", mapped_section[0])
        found_chapter = re.match(r"(CHAPTER\s(\w+))", mapped_section[0])

        if found_section:
            section = {
                "section_name": found_section.group(1),
                "description": found_section[1],
                "chapters": []
            }
            current_section = section

        elif found_chapter:
            chapter = {
                "chapter_name": found_chapter.group(1),
                "description": found_chapter[1],
            }
            chapters = list(map(lambda x: x.get("chapter_name"), current_section.get("chapters", [])))
            if len(last_chapter_name) > 0 and last_chapter_name not in chapters:
                # Push article if present
                if len(previous_article.keys()) > 0:
                    article_queue.append(previous_article)
                    previous_article = {}
                release(last_chapter_name, article_queue)

                # Reset current data
                current_heading = {}
                current_sub_heading_1 = {}
                current_sub_heading_2 = {}
                article_queue = []

            last_chapter_name = chapter["chapter_name"]
            if current_section.get("chapters"):
                current_section["chapters"].append(chapter)

        else:
            sections.append(current_section)

            try:
                possible_article = tb.read_pdf(file, area=(61.92, 138.24, 91.44, 429.12), pages=str(index), stream=True,
                            output_format="json")[0]["data"][0][0]["text"]
                if "Article Description" not in possible_article:
                    raise Exception("Not Article")
            except:
                continue

            # Extract from article
            rows = tb.read_pdf(file, area=(92.16, 33.84, 550.8, 805.68), pages=str(index), stream=True,
                               columns=column_coordinates,
                               output_format="json")[0]["data"]

            for idx, row in enumerate(rows):
                value = {}

                # Populate row
                for jdx, col in enumerate(row):
                    col_name = get_column_name(col["left"])
                    if col_name:
                        value[col_name] = col["text"]

                # populate values for heading from other values
                if not value.get("heading") and not value.get("article"):
                    # collect_header_data_from_columns(value)
                    if len(previous_article.keys()) > 0:
                        collect_article_data(previous_article, value)

                elif not value.get("heading") and value.get("article"):
                    if last_item_type == "heading":
                        collect_header_data_from_columns(value, current_heading)
                    elif last_item_type == "sub_heading_1":
                        collect_header_data_from_columns(value, current_sub_heading_1)
                    elif last_item_type == "sub_heading_2":
                        collect_header_data_from_columns(value, current_sub_heading_2)
                    elif last_item_type == "article" and len(previous_article.keys()) > 0:
                        collect_article_data(previous_article, value)

                elif value.get("heading") and not value.get("article"):
                    print(value)

                elif value.get("heading") and value.get("article"):
                    hsn_type = get_hsn_type(value["heading"], value["article"], value.get("cd", ""))
                    value["hsn_type"] = hsn_type

                    if hsn_type == "heading":
                        # Push article if present
                        if len(previous_article.keys()) > 0:
                            article_queue.append(previous_article)
                            previous_article = {}

                        collect_header_data_from_columns(value)
                        current_heading = value
                        last_item_type = "heading"

                    elif hsn_type == "sub_heading_1":
                        collect_header_data_from_columns(value)
                        current_sub_heading_1 = value
                        current_sub_heading_2 = {}
                        last_item_type = "sub_heading_1"

                    elif hsn_type == "sub_heading_2":
                        collect_header_data_from_columns(value)
                        current_sub_heading_2 = value
                        last_item_type = "sub_heading_2"

                    elif hsn_type == "article":
                        if len(current_heading.keys()) > 0:
                            if len(previous_article.keys()) > 0:
                                article_queue.append(previous_article)
                                previous_article = {}

                            previous_article = {
                                "id": str(uuid.uuid4()),
                                "heading_code": current_heading.get("heading", ""),
                                "sub_heading_1": current_sub_heading_1.get("heading", None),
                                "sub_heading_2": current_sub_heading_2.get("heading", None),
                                "code": value.get("heading", ""),
                                "main_head_description": current_heading.get("article", ""),
                                "sub_head_1_description": current_sub_heading_1.get("article", ""),
                                "sub_head_2_description": current_sub_heading_2.get("article", ""),
                                "description": value.get("article", ""),
                                "cd": value.get("cd", ""),
                                "general": value.get("general", ""),
                                "eu_uk": value.get("eu_uk", ""),
                                "efta": value.get("efta", ""),
                                "sadc": value.get("sadc", ""),
                                "mercosur": value.get("mercosur", ""),
                                "afcfta": value.get("afcfta", ""),
                                "statistical_unit": value.get("statistical_unit", ""),
                            }
                            last_item_type = "article"


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    extract('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
