"""
Using the standard Java serialization library, Spark objects are converted into
streams of bytes as they are moved around the network.

This approach may be slower, since serialized data is more CPU-intensive to read than
deserialized data; however, it is often more memory efficient, since it allows the user
to choose a more efficient representation.

While Java serialization is more efficient than full objects, Kryo serialization can be even more space efficient.

"""