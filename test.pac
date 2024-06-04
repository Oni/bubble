function FindProxyForURL(url, host) {
    if (shExpMatch(host, '*.my.cool.domain.com')) {
      return 'PROXY localhost:1090; DIRECT';
    }
  
    return 'DIRECT';
  }
