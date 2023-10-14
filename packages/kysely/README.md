# Usage/examples
```ts
// 1. Specify some entities and relations

class Book {
  public id: string;
  public title: string;
  public author_id: string;
  constructor(data) {
    Object.assign(this, data);
  }
}

const bookDef = {
  tableName: "books",
  // toEntity() converts a database row into your entity
  toEntity: (data: unknown) => new Book(data),
  // toInsert() converts its input into a database row to insert. The input type is used for type hints in orm.insert().
  toInsert: (data: Partial<Book>) => data,
  // toUpdate() converts its input into fields to update. The input type is used for type hints in orm.update().
  toUpdate: (data: Partial<Book>) => data,
} satisfies SnadiKyselyEntityDefinition;

const bookAuthor = (orm) => hasOne(bookDef, "author_id", authorDef, "id")(orm);
const bookPages = (orm) => hasMany(bookDef, "id", pageDef, "book_id")(orm);

class Page {
  public id: string;
  public content: string;
  public book_id: string;
  constructor(data) {
    Object.assign(this, data);
  }
}

const pageDef = {
  tableName: "pages",
  toEntity: (data: unknown) => new Page(data),
  toInsert: (data: Partial<Page>) => data,
  toUpdate: (data: Partial<Page>) => data,
};

class Author {
  public id: string;
  public name: string;
  constructor(data) {
    Object.assign(this, data);
  }
}

const authorDef = {
  tableName: "authors",
  toEntity: (data: unknown) => new Author(data),
  toInsert: (data: Partial<Author>) => data,
  toUpdate: (data: Partial<Author>) => data,
};

const authorBooks = (orm) => hasMany(authorDef, "id", bookDef, "author_id")(orm);

// 2. Create Kysely instance

type KyselyDB = EntitiesToKyselyDatabase<
  | typeof bookDef
  | typeof authorDef
>;
const dialect = new SqliteDialect({ /* ... */ });
const kysely = new Kysely<KyselyDB>({ dialect });

// 3. Create ORM instance

const orm = new SnadiKyselyOrm(kysely);

// 4. Use the ORM

const authors = await orm.getAll(authorDef, {
  books: [authorBooks(orm), {
    pages: bookPages(orm),
  }],
});
// typeof authors = Array<Author & { books: Array<Book & { pages: Page[] }> }>

const allPages = authors.map(author => author.books.map(book => book.pages)).flat(2);
// typeof allPages = Page[]

await orm.insert(authorDef, {
  // Type hinted with author's toInsert input
});

await orm.update(
  authorDef,
  // Kysely's query builder is used for specifying update conditions
  // This is fully type hinted by Kysely since we created the KyselyDB above from our entities
  qb => qb.where("id", "=", "1234"),
  {
    // Type hinted with author's toUpdate input
  }
);

const someAuthors = await orm.getMany(
  authorDef,
  // Kysely's query builder is used for specifying select conditions
  qb => qb.where(/* ... */),
);

const fromRawQuery = await orm.getMany(
  authorDef,
  // Using sql tag from Kysely
  () => sql`
    SELECT * FROM authors
  `,
  // Relations still work too!
  {
    books: authorBooks(orm)
  }
);
```
