# %%
import base64
import hashlib

# %%
# address_bytes = base64.b64decode('k3ZZQqtbbSRtH4CZAkG+XzTrdi/hWs8FrsnDFR3DlDE=')
address_bytes = base64.b64decode("9orEqXoI5YtIu44xk4Lg6dAod+0uRWs1nMpDiVbm8vs=")

# %%
arry = bytearray(32 + 4)
# %%
arry[:32] = address_bytes
# %%
sha = hashlib.sha512()
sha.update(address_bytes)

address_hash = sha.digest()
checksum = address_hash[len(address_hash) - 4 :]
# %%
arry[32:] = address_hash[len(address_hash) - 4 :]
# %%
addr_chcksum_bytes = base64.b32encode(arry)
hex = base64.b32encode(arry).hex()


# %%
bytes.fromhex(hex)
# %%
# len('47YPQTIGQEO7T4Y4RWDYWEKV6RTR2UNBQXBABEEGM72ESWDQNCQ52OPASU')
# 58
# %%
