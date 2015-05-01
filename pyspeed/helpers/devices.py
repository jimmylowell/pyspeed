devices = [['web_browser', '1'], ['android', '5'], ['iphone', '6'], ['windows_phone', '9']]
mobile_devices = [['android', '5'], ['iphone', '6'], ['windows_phone', '9']]

def yieldAll():
	for device in devices:
		yield device
def mobile_list():
	for mobile in mobile_devices:
		yield mobile

def listNames():
	for device in devices:
		yield device[0]