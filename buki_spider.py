import scrapy
import json

class BukiSpider(scrapy.Spider):
    name = "buki"
    allowed_domains = ["buki-kz.com"]
    start_urls = [f"https://buki-kz.com/repetitor/{i}" for i in range(1, 151)]
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 1,
    }
    
    def __init__(self, *args, **kwargs):
        super(BukiSpider, self).__init__(*args, **kwargs)
        self.items = []
        self.seen_tutors = set() 
    
    def parse(self, response):
        cards = response.css("div.styles_card__Yjci5")
        self.logger.info(f'Found {len(cards)} tutors on {response.url}')
        
        for card in cards:
            # Extract experience
            experience_text = card.xpath('.//p[contains(@class, "styles_practice__AZyXc")]/text()').get(default='')
            experience = experience_text.replace("Опыт: ", "").strip() if experience_text else None
            
            # Extract price as integer
            price_text = card.css("span.topCeil::text").get(default='0')
            try:
                price = int(''.join(filter(str.isdigit, price_text))) if price_text and price_text.strip() else None
            except (ValueError, TypeError):
                price = None
            
            # Extract rating
            rating_text = card.css("div.styles_reviewsBlock__FNrPL span::text").re_first(r"(\d\.\d)")
            try:
                rating = float(rating_text) if rating_text else 0.0
            except (ValueError, TypeError):
                rating = 0.0
            
            # Extract review count
            review_text = card.css("div.styles_reviewsBlock__FNrPL span.styles_reviewsCount__EAIh6::text").re_first(r"\d+")
            try:
                review_count = int(review_text) if review_text else 0
            except (ValueError, TypeError):
                review_count = 0
            
            item = {
                # Core tutor info
                "subjects": card.css("span.styles_lessonsItem__v8FAD::text").getall(),
                "education": card.css("p.styles_education__41VXk span::text").get(),
                "experience": experience,
                
                # Location and pricing
                "city": card.css("a.styles_link__5pWac::text").get(),
                "price": price,
                
                # Ratings and reviews
                "rating": rating,
                "reviewCount": review_count,
                
                # Additional details
                "shortDescription": card.css("span.styles_shortDescription__9jRi6::text").get(),
                
                # Feature flags
                "canWorkOnline": bool(card.css("p.styles_workOnline__p4t8f")),
                "isVerified": bool(card.css("p.styles_veryfied__WfcBr")),
                "hasFreeTrial": bool(card.css("p.styles_freeLesson__yIPfq")),
            }
            
            # Unique identifier for deduplication
            tutor_id = (
                tuple(sorted(item['subjects'])),
                item['education'],
                item['shortDescription'],
                item['price']
            )
            
            # Only add if not seen before
            if tutor_id not in self.seen_tutors:
                self.seen_tutors.add(tutor_id)
                self.items.append(item)
                yield item
            else:
                self.logger.debug(f'Skipping duplicate: {item["education"]}')
    
    def closed(self, reason):
        # Write JSON file
        with open('buki_tutors_data.jsonn', 'w', encoding='utf-8') as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f'Saved {len(self.items)} data to buki_tutors_data.json')