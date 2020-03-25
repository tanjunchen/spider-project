function getGuid(){
    var createGuid = function() {
		return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
	};
    var guid1 = createGuid() + createGuid() + "-" + createGuid() + "-" + createGuid() + createGuid() + "-" + createGuid() + createGuid() + createGuid();
    return  guid1;
};
