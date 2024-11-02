pub trait MapNone {
    fn map_none<F>(self, f: F) -> Self
    where
        F: FnOnce();
}

impl<T> MapNone for Option<T> {
    fn map_none<F>(self, f: F) -> Self
    where
        F: FnOnce(),
    {
        if self.is_none() {
            f();
        }
        self
    }
}
