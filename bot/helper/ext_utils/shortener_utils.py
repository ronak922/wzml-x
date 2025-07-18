from base64 import b64encode
from random import choice, random
from asyncio import sleep as asleep
from urllib.parse import quote
import re

from cloudscraper import create_scraper
from urllib3 import disable_warnings

from ... import LOGGER, shortener_dict


def is_valid_url(url):
    """Check if URL is valid"""
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return url_pattern.match(url) is not None


async def short_url(longurl, attempt=0):
    if not shortener_dict:
        LOGGER.warning("⚠️ No shorteners configured, returning original URL")
        return longurl
    if attempt >= 4:
        LOGGER.error(f"❌ Max attempts reached for shortening: {longurl}")
        return longurl
    
    _shortener, _shortener_api = choice(list(shortener_dict.items()))
    LOGGER.info(f"🔗 Attempting to shorten with: {_shortener}")
    
    cget = create_scraper().request
    disable_warnings()
    try:
        if "shorte.st" in _shortener:
            headers = {"public-api-token": _shortener_api}
            data = {"urlToShorten": quote(longurl)}
            response = cget(
                "PUT", "https://api.shorte.st/v1/data/url", headers=headers, data=data
            )
            LOGGER.info(f"📝 Shorte.st response: {response.text}")
            result = response.json()["shortenedUrl"]
            
        elif "linkvertise" in _shortener:
            url = quote(b64encode(longurl.encode("utf-8")))
            linkvertise = [
                f"https://link-to.net/{_shortener_api}/{random() * 1000}/dynamic?r={url}",
                f"https://up-to-down.net/{_shortener_api}/{random() * 1000}/dynamic?r={url}",
                f"https://direct-link.net/{_shortener_api}/{random() * 1000}/dynamic?r={url}",
                f"https://file-link.net/{_shortener_api}/{random() * 1000}/dynamic?r={url}",
            ]
            result = choice(linkvertise)
            
        elif "bitly.com" in _shortener:
            headers = {"Authorization": f"Bearer {_shortener_api}"}
            response = cget(
                "POST",
                "https://api-ssl.bit.ly/v4/shorten",
                json={"long_url": longurl},
                headers=headers,
            )
            LOGGER.info(f"📝 Bitly response: {response.text}")
            result = response.json()["link"]
            
        elif "ouo.io" in _shortener:
            response = cget(
                "GET", f"http://ouo.io/api/{_shortener_api}?s={longurl}", verify=False
            )
            LOGGER.info(f"📝 Ouo.io response: {response.text}")
            result = response.text.strip()
            
        elif "cutt.ly" in _shortener:
            response = cget(
                "GET",
                f"http://cutt.ly/api/api.php?key={_shortener_api}&short={longurl}",
            )
            LOGGER.info(f"📝 Cutt.ly response: {response.text}")
            result = response.json()["url"]["shortLink"]
            
        elif "vplink" in _shortener:
            # Correct VPLink API endpoint
            response = cget(
                "GET", 
                f"https://vplink.in/api?api={_shortener_api}&url={quote(longurl)}"
            )
            LOGGER.info(f"📝 VPLink response: {response.text}")
            json_resp = response.json()
            
            if json_resp.get("status") == "success":
                result = json_resp["shortenedUrl"]
            else:
                LOGGER.error(f"❌ VPLink API error: {json_resp.get('message', 'Unknown error')}")
                result = longurl
            
        elif "linkshortify" in _shortener:
            # Try correct LinkShortify API endpoint
            response = cget(
                "GET",
                f"https://linkshortify.com/api?api={_shortener_api}&url={quote(longurl)}"
            )
            LOGGER.info(f"📝 LinkShortify response: {response.text}")
            
            try:
                json_resp = response.json()
                if json_resp.get("status") == "success":
                    result = json_resp["shortenedUrl"]
                else:
                    LOGGER.error(f"❌ LinkShortify API error: {json_resp.get('message', 'Unknown error')}")
                    result = longurl
            except:
                # If not JSON, might be direct text response
                text_resp = response.text.strip()
                if text_resp.startswith("http"):
                    result = text_resp
                else:
                    LOGGER.error(f"❌ LinkShortify unexpected response: {text_resp[:100]}...")
                    result = longurl
            
        elif "is.gd" in _shortener:
            response = cget(
                "GET", f"https://is.gd/create.php?format=simple&url={quote(longurl)}"
            )
            result = response.text.strip()
            
        elif "tinyurl.com" in _shortener:
            response = cget(
                "GET", f"https://tinyurl.com/api-create.php?url={quote(longurl)}"
            )
            result = response.text.strip()
            
        else:
            # Generic shortener code
            response = cget(
                "GET",
                f"https://{_shortener}/api?api={_shortener_api}&url={quote(longurl)}",
            )
            LOGGER.info(f"📝 Generic shortener response: {response.text}")
            res = response.json()
            result = res.get("shortenedUrl", "")
            if not result:
                shrtco_res = cget(
                    "GET", f"https://api.shrtco.de/v2/shorten?url={quote(longurl)}"
                ).json()
                shrtco_link = shrtco_res["result"]["full_short_link"]
                res = cget(
                    "GET",
                    f"https://{_shortener}/api?api={_shortener_api}&url={shrtco_link}",
                ).json()
                result = res.get("shortenedUrl", "")
            if not result:
                result = longurl

        # Validate the result
        LOGGER.info(f"🔍 Shortened URL result: {result}")
        
        if not result or result == longurl:
            LOGGER.warning(f"⚠️ Shortener returned empty or same URL")
            return longurl
            
        if not is_valid_url(result):
            LOGGER.error(f"❌ Invalid URL returned: {result[:100]}...")
            return longurl
            
        # Additional checks for common error responses
        if any(error in result.lower() for error in ['error', 'invalid', 'failed', 'not found', 'html', 'doctype']):
            LOGGER.error(f"❌ Error response from shortener: {result[:100]}...")
            return longurl
            
        LOGGER.info(f"✅ Successfully shortened: {longurl} -> {result}")
        return result
        
    except Exception as e:
        LOGGER.error(f"❌ Shortener error with {_shortener}: {e}")
        await asleep(0.8)
        attempt += 1
        return await short_url(longurl, attempt)
