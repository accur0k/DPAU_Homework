import scrapy
import json
import re

class PreplySpider(scrapy.Spider):
    name = "preply"
    allowed_domains = ["preply.com"]
    
    start_urls = [
        "https://preply.com/ru/online/repetitory--matematika?CoB=KZ",
        "https://preply.com/ru/online/repetitory--angliyskogo?CoB=KZ",
        "https://preply.com/ru/online/repetitory--kazakhskii?CoB=KZ",
        "https://preply.com/ru/online/repetitory--khimiia?CoB=KZ",
        "https://preply.com/ru/online/repetitory--fizika?CoB=KZ",
        "https://preply.com/ru/online/repetitory--nemetskogo?CoB=KZ",
        "https://preply.com/ru/online/repetitory--informatika?CoB=KZ",
        "https://preply.com/ru/online/repetitory--russkogo?CoB=KZ",
        "https://preply.com/ru/online/repetitory--frantsuzskogo?CoB=KZ",
        "https://preply.com/ru/online/repetitory--ispanskogo?CoB=KZ",
        "https://preply.com/ru/online/repetitory--kitayskogo?CoB=KZ",
        "https://preply.com/ru/online/repetitory--koreiskii?CoB=KZ",
        "https://preply.com/ru/online/repetitory--turetskii?CoB=KZ",
        "https://preply.com/ru/online/repetitory--polskogo?CoB=KZ",
        "https://preply.com/ru/online/repetitory--cheshskii?CoB=KZ",
        "https://preply.com/ru/online/repetitory--informatika?CoB=KZ",
    ]

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_DELAY': 1.5,
    }
    
    def __init__(self, *args, **kwargs):
        super(PreplySpider, self).__init__(*args, **kwargs)
        self.items = []
        self.seen_tutors = set() 
    
    def parse(self, response):
        self.logger.info(f'Processing: {response.url}')
        
        cards_container = response.xpath('//ul[@data-qa-group="tutor-cards"]/div/li')
        if not cards_container:
             cards_container = response.xpath('//ul[@data-qa-group="tutor-cards"]/li')

        self.logger.info(f'Found {len(cards_container)} tutors on {response.url}')
        
        for card in cards_container:
            # Имя
            name_el = card.xpath('.//a[@data-clickable-element-name="name"]')
            tutor_name = name_el.xpath('.//h4/text()').get()

            # headline 
            headline_raw = card.xpath('.//div[@data-qa-id="seo-snippet-block"]//p//text()').get()
            headline = headline_raw.strip() if headline_raw else None
            
            if headline and ' — ' in headline:
                headline = headline.split(' — ')[0].strip()
                
            # Цена
            price_text = card.xpath('.//*[@data-qa-group="tutor-price-value"]/span/text()').get()
            try:
                if price_text:
                    price_digits = re.sub(r'[^\d]', '', price_text)
                    price = int(price_digits) if price_digits else None
                else:
                    price = None
            except (ValueError, TypeError):
                price = None

            # Рейтинг
            rating_text = card.xpath('.//button[contains(@class, "RatingIndicator")]//h4/text()').get()
            if not rating_text:
                 rating_text = card.xpath('.//button[contains(@class, "reviewsButton")]//h5/text()').get()

            try:
                rating = float(rating_text) if rating_text else 0.0
            except (ValueError, TypeError):
                rating = 0.0

            # Количество отзывов
            reviews_text = card.xpath('.//p[contains(text(), "отзыв")]/text()').get()
            review_count = 0
            if reviews_text:
                reviews_match = re.search(r'(\d+)\s*отзыв', reviews_text)
                if reviews_match:
                    review_count = int(reviews_match.group(1))

            # Страна
            country = card.xpath('.//img[contains(@class, "flag")]/@alt').get()

            # Предметы
            subjects = card.xpath('.//ul//li//p/text()').getall()
            subjects = [s.strip() for s in subjects if s.strip()]
            
            # Верификация
            is_verified = bool(card.xpath('.//div[contains(@class, "SearchCardHeadingBadges")]//span[contains(@class, "Icon")]'))
            
            item = {
                "subjects": subjects,
                "country": country, 
                "price": price,
                "rating": rating,
                "reviewCount": review_count,
                "shortDescription": headline,
                "canWorkOnline": True,
                "isVerified": is_verified,
            }
            
            # Дедупликация
            if tutor_name:
                tutor_id = (tutor_name, item['shortDescription'], str(item['price']))
                
                if tutor_id not in self.seen_tutors:
                    self.seen_tutors.add(tutor_id)
                    self.items.append(item)
                    yield item
                else:
                    self.logger.debug(f'Skipping duplicate: {tutor_name}')

        # Пагинация
        current_page_match = re.search(r'page=(\d+)', response.url)
        current_page = int(current_page_match.group(1)) if current_page_match else 1
        
        next_page_url = None
        max_pages = 15
        
        if len(cards_container) > 0 and current_page < max_pages:
            if 'page=' in response.url:
                next_page_url = re.sub(r'page=\d+', f'page={current_page + 1}', response.url)
            else:
                separator = '&' if '?' in response.url else '?'
                next_page_url = response.url + f'{separator}page={current_page + 1}'
        
        if next_page_url:
            self.logger.info(f'Going to next page: {next_page_url}')
            yield response.follow(next_page_url, callback=self.parse)
    
    def closed(self, reason):
        with open('preply_tutors_data.json', 'w', encoding='utf-8') as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f'Saved {len(self.items)} tutors to preply_tutors_data.json')