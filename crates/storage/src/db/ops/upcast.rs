use sqd_array::builder::AnyBuilder;


struct UpcastReader<S> {
    src: S,
    src_buf: AnyBuilder
}