# ifsc-results-scraper

`ifsc-results-scraper` is a Python-based tool designed to scrape and process competition results from the International Federation of Sport Climbing (IFSC) website. It automates the extraction of data, making it easier to analyze and work with climbing competition results.

The scraper is a python script in `./src`. It loads the headers for the request from `config.yml`. You should also fetch a cookie-string and add it to the header. You can find a valid cookie-string by going to [ifsc.result.info](https://ifsc.results.info) and using *developer tools* in your browser. Add it to a `.env` file in the parent folder and the script should work although you might need to also update some of the other parameters in the header.

**Disclaimer**: This project is not affiliated with or endorsed by the IFSC. Use it responsibly and ensure compliance with the website's terms of service.