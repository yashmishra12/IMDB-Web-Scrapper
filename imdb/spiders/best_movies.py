import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class BestMoviesSpider(CrawlSpider):
    name = 'best_movies'
    allowed_domains = ['imdb.com']

    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36'

    def start_requests(self):
        yield scrapy.Request(url = 'https://www.imdb.com/search/title/?groups=top_250&sort=user_rating', headers = {
            'User-Agent': self.user_agent
        })

    rules = (
        Rule(LinkExtractor(restrict_xpaths=("//h3[@class='lister-item-header']/a")), callback='parse_item', follow=True, process_request='set_user_agent'),
        Rule(LinkExtractor(restrict_xpaths=('(//a[@class="lister-page-next next-page"])[1]')), process_request = 'set_user_agent' )
    ) #LinkExtractor automatically searches for href, so don't add href
#Order of rule is important. So, call pagination rule only after scrapping the first page
    
    def set_user_agent (self, request, spider):
        request.headers['User-Agent'] = self.user_agent
        return request

    def parse_item(self, response):
        yield {
            'title' : response.xpath('//div[@class="title_wrapper"]/h1/text()').get(),
            'year' : response.xpath('//span[@id="titleYear"]/a/text()').get(),
            'duration' : response.xpath('normalize-space((//time)[1]/text())').get(),
            'genre' : response.xpath('(//div[@class="subtext"]/a)[1]/text()').get(),
            'rating' : response.xpath('//span[@itemprop="ratingValue"]/text()').get(),
            'release_date': response.xpath('//a[@title="See more release dates"]/text()').get()  
        }
