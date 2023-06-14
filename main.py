import datetime
import json
import uuid
import tabula as tb
from PyPDF2 import PdfReader
import re
import io

column_coordinates = [34, 102.24, 136.8, 428.4, 482.4, 531.36, 582.48, 634.32, 685.44, 753.84]
column_names = ["heading", "code", "article", "statistical_unit", "general", "eu_uk", "efta", "sadc", "mercosur",
                "afcfta"]
heading_data_keys = ["article", "statistical_unit", "general", "eu_uk", "efta", "sadc", "mercosur", "afcfta"]


def get_hsn_type(hsn_number: str, article: str):
    partitioned_hsn = hsn_number.split('.')
    if len(partitioned_hsn[0]) == 2:
        return "heading"
    elif re.search(r"(.)*:", article):
        return "sub_heading"
    else:
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
        return "code"
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
    if key in column_names and value == "free":
        return 0
    elif key == "article":
        value = value.replace("-", "")
        value = value.strip()
    return value


def release(filename: str, data: list):
    with io.open("./output/" + filename + '.json', 'w', encoding='utf8') as outfile:
        str_ = json.dumps(data, indent=2, ensure_ascii=False)
        outfile.write(str_)


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
    current_section: any = {}
    last_chapter_name = ""
    articles = []
    article_queue = []
    heading_row = {}
    sub_heading_row = {}

    for index in range(pdf_page_count):
        if index < 2:
        # if index < 623:
            continue
        # if index == 626:
        #     break

        print("Page " + str(index))
        possible_section = tb.read_pdf(file, area=(57.6, 34.56, 92.16, 806.4), pages=str(index), stream=True,
                                       output_format="json")[0]["data"]
        # print(possible_section)
        mapped_section = list(map(lambda x: x[0]["text"], possible_section))
        # print(mapped_section)
        found_section = re.match(r"(SECTION\s(\w+))", mapped_section[0])
        found_chapter = re.match(r"(CHAPTER\s(\w+))", mapped_section[0])
        if found_section:
            section = {
                "section_name": found_section.group(1),
                "description": found_section[1],
                "chapters": []
            }
            current_section = section
            # print(section)
        elif found_chapter:
            chapter = {
                "chapter_name": found_chapter.group(1),
                "description": found_chapter[1],
            }
            if len(last_chapter_name) > 0 and last_chapter_name not in list(map(lambda x: x.get("chapter_name"), current_section["chapters"])):
                release(last_chapter_name, article_queue)
                article_queue = []
            last_chapter_name = chapter["chapter_name"]
            if current_section.get("chapters"):
                current_section["chapters"].append(chapter)
            # print(chapter)
        else:
            sections.append(current_section)


            try:
                possible_article = tb.read_pdf(file, area=(61.92, 138.24, 91.44, 429.12), pages=str(index), stream=True,
                            output_format="json")[0]["data"][0][0]["text"]
                if "Article Description" not in possible_article:
                    raise Exception("Not Article")
            except:
                continue

            rows = tb.read_pdf(file, area=(92.16, 33.84, 550.8, 805.68), pages=str(index), stream=True,
                               columns=column_coordinates,
                               output_format="json")[0]["data"]
            last_article: dict
            for idx, row in enumerate(rows):
                value = {}
                for jdx, col in enumerate(row):
                    col_name = get_column_name_quick(col["left"])
                    if col_name:
                        value[col_name] = process_raw_data(col_name, col["text"])
                if value.get("heading") and not value.get("article"):
                    print(value)
                if value.get("heading") and value.get("article"):
                    hsn_type = get_hsn_type(value["heading"], value["article"])
                    value["hsn_type"] = hsn_type
                    if hsn_type == "heading":
                        for key in value.keys():
                            if key in heading_data_keys:
                                value["article"] += " " + value[key].strip()
                        heading_row = value
                    elif hsn_type == "sub_heading":
                        sub_heading_row = value
                        for key in value.keys():
                            if key in heading_data_keys:
                                value["article"] += " " + str(value[key]).strip()
                    elif hsn_type == "article":
                        if len(heading_row.keys()) > 0:
                            article = {
                                "id": str(uuid.uuid4()),
                                "heading_code": heading_row.get("heading", ""),
                                "sub_heading_code": sub_heading_row.get("heading", ""),
                                "code": value.get("heading", ""),
                                "main_head_description": heading_row.get("article", ""),
                                "sub_head_description": sub_heading_row.get("article", ""),
                                "description": value.get("article", ""),
                                "cd": value.get("code", ""),
                                "general": value.get("general", ""),
                                "eu_uk": value.get("eu_uk", ""),
                                "efta": value.get("efta", ""),
                                "sadc": value.get("sadc", ""),
                                "mercosur": value.get("mercosur", ""),
                                "afcfta": value.get("afcfta", ""),
                                "statistical_unit": value.get("statistical_unit", ""),
                            }
                            last_article = article
                            # articles.append(article)
                            article_queue.append(article)
                elif last_article:
                    keys = value.keys()
                    if value.get("article"):
                        last_article["description"] += " " + value.get("article")
                    for key in keys:
                        if last_article.get(key):
                            last_article[key] += " " + value.get(key)
                # print("row", value)

            # print(json.dumps(cols, indent=2))
    # print(sections)
    # print(json.dumps(articles, indent=2))

    # with io.open('output.json', 'w', encoding='utf8') as outfile:
    #     str_ = json.dumps(articles, indent=2, ensure_ascii=False)
    #     outfile.write(str_)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    extract('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
