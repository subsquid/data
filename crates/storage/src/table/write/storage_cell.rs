use crate::kv::KvWrite;
use std::cell::RefCell;
use std::rc::Rc;


pub struct StorageCell<S> {
    inner: Rc<RefCell<S>>
}


impl<S> Clone for StorageCell<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner)
        }
    }
}


impl<S: KvWrite> StorageCell<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: Rc::new(RefCell::new(inner))
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        (&self.inner).borrow_mut().put(key, value)
    }
    
    pub fn into_inner(self) -> S {
        Rc::into_inner(self.inner)
            .expect("storage is still in use")
            .into_inner()
    }
}


impl<S: KvWrite> KvWrite for StorageCell<S> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        (&*self).put(key, value)
    }
}