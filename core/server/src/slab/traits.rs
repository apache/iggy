pub trait ProjectCell {
    type View<'me>
    where
        Self: 'me;
    fn project(&self) -> Self::View<'_>;
}

pub trait ProjectCellMut {
    type ViewMut<'me>
    where
        Self: 'me;
    fn project_mut(&self) -> Self::ViewMut<'_>;
}

pub trait Project {
    type View<'me>
    where
        Self: 'me;
    fn project(&self) -> Self::View<'_>;
}

pub trait ProjectMut {
    type ViewMut<'me>
    where
        Self: 'me;
    fn project_mut(&mut self) -> Self::ViewMut<'_>;
}

pub trait Decompose {
    type Target;
    fn decompose(self) -> Self::Target;
}

pub trait Insert<I>
where
    I: Decompose,
{
    fn insert(&mut self, item: I) -> usize;
}

pub struct NonCell;
pub struct Cell;

pub trait Access<Ref, Marker>
where
    Ref: Decompose,
{
    fn with<T, F>(self, f: F) -> T
    where
        F: FnOnce(Ref::Target) -> T;

    fn with_async<T, F>(self, f: F) -> impl Future<Output = T>
    where
        F: AsyncFnOnce(Ref::Target) -> T;
}

pub trait AccessMut<RefMut, Marker>
where
    RefMut: Decompose,
{
    fn with_mut<T, F>(self, f: F) -> T
    where
        F: FnOnce(RefMut::Target) -> T;
}

impl<'slab, Slab, Ref> Access<Ref, NonCell> for &'slab Slab
where
    Ref: Decompose,
    Slab: Project<View<'slab> = Ref>,
{
    fn with<T, F>(self, f: F) -> T
    where
        F: FnOnce(Ref::Target) -> T,
    {
        f(self.project().decompose())
    }

    async fn with_async<T, F>(self, f: F) -> T
    where
        F: AsyncFnOnce(Ref::Target) -> T,
    {
        f(self.project().decompose()).await
    }
}

impl<'slab, Slab, RefMut> AccessMut<RefMut, NonCell> for &'slab mut Slab
where
    RefMut: Decompose,
    Slab: ProjectMut<ViewMut<'slab> = RefMut>,
{
    fn with_mut<T, F>(self, f: F) -> T
    where
        F: FnOnce(RefMut::Target) -> T,
    {
        f(self.project_mut().decompose())
    }
}

impl<'slab, Slab, Ref> Access<Ref, Cell> for &'slab Slab
where
    Ref: Decompose,
    Slab: ProjectCell<View<'slab> = Ref>,
{
    fn with<T, F>(self, f: F) -> T
    where
        F: FnOnce(Ref::Target) -> T,
    {
        f(self.project().decompose())
    }

    async fn with_async<T, F>(self, f: F) -> T
    where
        F: AsyncFnOnce(Ref::Target) -> T,
    {
        f(self.project().decompose()).await
    }
}

impl<'slab, Slab, RefMut> AccessMut<RefMut, Cell> for &'slab Slab
where
    RefMut: Decompose,
    Slab: ProjectCellMut<ViewMut<'slab> = RefMut>,
{
    fn with_mut<T, F>(self, f: F) -> T
    where
        F: FnOnce(RefMut::Target) -> T,
    {
        f(self.project_mut().decompose())
    }
}
