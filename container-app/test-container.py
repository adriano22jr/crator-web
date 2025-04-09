import requests

# Local test
def local_test():
    TOR_PROXY = "socks5h://localhost:9050"
    url = "https://check.torproject.org"

    response = requests.get(url, proxies={"http": TOR_PROXY, "https": TOR_PROXY})
    print(response.text)


# Azure test
def azure_test():
    proxies = {
        "http": "socks5h://tor-proxy.gentlesea-22ce755d.northeurope.azurecontainerapps.io:9050",
        "https": "socks5h://tor-proxy.gentlesea-22ce755d.northeurope.azurecontainerapps.io:9050",
    }

    response = requests.get("https://check.torproject.org", proxies = proxies)
    print(response.text)


if __name__ == "__main__":
    # Uncomment the test you want to run
    # local_test()
    azure_test()