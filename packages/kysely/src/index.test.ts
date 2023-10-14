import test, { after } from "node:test";
import SQLite from "better-sqlite3";
import { EntitiesToKyselyDatabase, SnadiKyselyEntityDefinition, ValidSnadiKyselyEntityDefinition, createKyselyOrm, hasMany, hasOne } from "./index.js";
import assert from "node:assert";
import { InsertResult, Kysely, SqliteDialect } from "kysely";

test("tests", async (t) => {
  // ENTITIES

  const toEntityClass = <T>(theClass: new () => T, props: any): T => {
    const instance = new theClass();
    for (const [key, value] of Object.entries(props)) {
      (instance as any)[key] = value;
    }
    return instance;
  };

  class Bookstore {
    id!: number;
    name!: string;
  }

  const bookstoreDef = {
    tableName: "bookstores" as const,
    toEntity: (data: Bookstore) => toEntityClass(Bookstore, data),
    toInsert: (data: Partial<Bookstore>) => data,
    toUpdate: (data: Partial<Bookstore>) => data,
  } satisfies SnadiKyselyEntityDefinition;

  const bookstoreBooks = () => hasMany(bookstoreDef, "id", bookDef, "bookstore_id");

  class Book {
    declare id: number;
    declare title: string;
    declare author_id: number;
    declare bookstore_id: number;
  }

  const bookDef = {
    tableName: "books" as const,
    toEntity: (data: Book) => toEntityClass(Book, data),
    toInsert: (data: Partial<Book>) => data,
    toUpdate: (data: Partial<Book>) => data,
  } satisfies SnadiKyselyEntityDefinition;

  const bookBookDetails = () => hasOne(bookDef, "id", bookDetailsDef, "book_id");

  const bookAuthor = () => hasOne(bookDef, "author_id", authorDef, "id");

  class BookDetails {
    declare id: number;
    declare book_id: number;
    declare isbn: string;
  };

  const bookDetailsDef = {
    tableName: "book_details" as const,
    toEntity: (data: BookDetails) => toEntityClass(BookDetails, data),
    toInsert: (data: Partial<BookDetails>) => data,
    toUpdate: (data: Partial<BookDetails>) => data,
  } satisfies SnadiKyselyEntityDefinition;

  class Author {
    declare id: number;
    declare name: string;
  }

  const authorDef = {
    tableName: "authors" as const,
    toEntity: (data: Author) => toEntityClass(Author, data),
    toInsert: (data: Partial<Author>) => data,
    toUpdate: (data: Partial<Author>) => data,
  } satisfies SnadiKyselyEntityDefinition;

  const authorBooks = () => hasMany(authorDef, "id", bookDef, "author_id");

  // INITIALIZE KYSELY AND ORM

  type KyselyDB = EntitiesToKyselyDatabase<
    | typeof bookstoreDef
    | typeof bookDef
    | typeof bookDetailsDef
    | typeof authorDef
  >;

  const dialect = new SqliteDialect({
    database: new SQLite(":memory:"),
  });

  const kysely = new Kysely<KyselyDB>({ dialect });

  after(() => {
    kysely.destroy();
  });

  const orm = createKyselyOrm(kysely);

  // DATABASE TABLES

  await kysely.schema
    .createTable("bookstores")
    .addColumn("id", "integer", c => c.primaryKey().autoIncrement())
    .addColumn("name", "text")
    .execute();

  await kysely.schema
    .createTable("books")
    .addColumn("id", "integer", c => c.primaryKey().autoIncrement())
    .addColumn("title", "text")
    .addColumn("bookstore_id", "integer")
    .addColumn("author_id", "integer")
    .execute();

  await kysely.schema
    .createTable("book_details")
    .addColumn("id", "integer", c => c.primaryKey().autoIncrement())
    .addColumn("book_id", "integer", c => c.unique())
    .addColumn("isbn", "text")
    .execute();

  await kysely.schema
    .createTable("authors")
    .addColumn("id", "integer", c => c.primaryKey().autoIncrement())
    .addColumn("name", "text")
    .execute();

  // CREATE TEST DATA

  const fromInsert = async <EntityDef extends ValidSnadiKyselyEntityDefinition<KyselyDB>>(def: EntityDef, insertResult: InsertResult) => {
    return (await orm.getOne(def, qb => qb.where("id", "=", Number(insertResult.insertId) as any)))!;
  };

  const createdAuthors = {
    neil: await fromInsert(authorDef, await orm.insert(authorDef, {
      name: "Neil Gaiman",
    })),
    pratchett: await fromInsert(authorDef, await orm.insert(authorDef, {
      name: "Terry Pratchett",
    })),
  };

  const createdBookstores = {
    yeOldeBookShoppe: await fromInsert(bookstoreDef, await orm.insert(bookstoreDef, {
      name: "Ye Olde Book Shoppe",
    })),
    brigittesBooks: await fromInsert(bookstoreDef, await orm.insert(bookstoreDef, {
      name: "Brigitte's Books",
    })),
    noBooks: await fromInsert(bookstoreDef, await orm.insert(bookstoreDef, {
      name: "No Books Store",
    })),
  };

  const createdBooks = {
    fragileThings: await fromInsert(bookDef, await orm.insert(bookDef, {
      author_id: createdAuthors.neil.id,
      bookstore_id: createdBookstores.brigittesBooks.id,
      title: "Fragile Things",
    })),
    theColourOfMagic: await fromInsert(bookDef, await orm.insert(bookDef, {
      author_id: createdAuthors.pratchett.id,
      bookstore_id: createdBookstores.yeOldeBookShoppe.id,
      title: "The Colour of Magic",
    })),
    guardsGuards: await fromInsert(bookDef, await orm.insert(bookDef, {
      author_id: createdAuthors.pratchett.id,
      bookstore_id: createdBookstores.brigittesBooks.id,
      title: "Guards! Guards!",
    })),
    theLastHero: await fromInsert(bookDef, await orm.insert(bookDef, {
      author_id: createdAuthors.pratchett.id,
      bookstore_id: createdBookstores.brigittesBooks.id,
      title: "The Last Hero",
    })),
  };

  const createdBookDetails = {
    fragileThings: await fromInsert(bookDetailsDef, await orm.insert(bookDetailsDef, {
      book_id: createdBooks.fragileThings.id,
      isbn: "0-06-051522-8",
    })),
    theColourOfMagic: await fromInsert(bookDetailsDef, await orm.insert(bookDetailsDef, {
      book_id: createdBooks.theColourOfMagic.id,
      isbn: "0-86140-324-X",
    })),
  };

  await t.test("orm.insert()", async (t) => {
    const authors = await orm.getAll(authorDef);
    assert.strictEqual(authors.length, Object.keys(createdAuthors).length);

    const bookstores = await orm.getAll(bookstoreDef);
    assert.strictEqual(bookstores.length, Object.keys(createdBookstores).length);

    const books = await orm.getAll(bookDef);
    assert.strictEqual(books.length, Object.keys(createdBooks).length);

    const bookDetails = await orm.getAll(bookDetailsDef);
    assert.strictEqual(bookDetails.length, Object.keys(createdBookDetails).length);
  });

  await t.test("1:1 relations", async (t) => {
    await t.test("orm.getAll()", async (t) => {
      const books = await orm.getAll(bookDef, {
        details: bookBookDetails()(orm),
      });
      assert.strictEqual(books.length, 4);
      for (const book of books) {
        assert.ok(book instanceof Book);
        assert.ok("details" in book);
        assert.ok(book.details == null || book.details instanceof BookDetails);
      }
    });

    await t.test("orm.getMany()", async (t) => {
      const bookIdsToFetch = [createdBooks.fragileThings.id, createdBooks.guardsGuards.id];
      const books = await orm.getMany(bookDef, qb => qb.where("id", "in", bookIdsToFetch), {
        details: bookBookDetails()(orm),
      });
      assert.strictEqual(books.length, bookIdsToFetch.length);
      for (const book of books) {
        assert.ok(book instanceof Book);
        assert.ok("details" in book);
        assert.ok(book.details == null || book.details instanceof BookDetails);
      }
    });

    await t.test("orm.getOne()", async (t) => {
      const fragileThings = await orm.getOne(bookDef, qb => qb.where("id", "=", createdBooks.fragileThings.id), {
        details: bookBookDetails()(orm),
      });
      assert.ok(fragileThings instanceof Book);
      assert.strictEqual(fragileThings.details!.isbn, createdBookDetails.fragileThings.isbn);
    });
  });

  await t.test("1:m relations", async (t) => {
    await t.test("orm.getAll()", async (t) => {
      const bookstores = await orm.getAll(bookstoreDef, {
        books: bookstoreBooks()(orm),
      });
      assert.strictEqual(bookstores.length, Object.keys(createdBookstores).length);
      for (const bookstore of bookstores) {
        assert.ok(bookstore instanceof Bookstore);
        assert.ok(Array.isArray(bookstore.books));
        for (const book of bookstore.books) {
          assert.ok(book instanceof Book);
          assert.strictEqual(book.bookstore_id, bookstore.id);
        }
      }
    });

    await t.test("orm.getMany()", async (t) => {
      const bookstoreIdsToFetch = [createdBookstores.brigittesBooks.id, createdBookstores.noBooks.id];
      const bookstores = await orm.getMany(bookstoreDef, qb => qb.where("id", "in", bookstoreIdsToFetch), {
        books: bookstoreBooks()(orm),
      });
      assert.strictEqual(bookstores.length, bookstoreIdsToFetch.length);
      for (const bookstore of bookstores) {
        assert.ok(bookstore instanceof Bookstore);
        assert.ok(Array.isArray(bookstore.books));
        for (const book of bookstore.books) {
          assert.ok(book instanceof Book);
          assert.strictEqual(book.bookstore_id, bookstore.id);
        }
      }
    });

    await t.test("orm.getOne()", async (t) => {
      const bookstore = await orm.getOne(bookstoreDef, qb => qb.where("id", "=", createdBookstores.brigittesBooks.id), {
        books: bookstoreBooks()(orm),
      });
      assert.ok(bookstore instanceof Bookstore);
      assert.ok(Array.isArray(bookstore.books));
      assert.strictEqual(bookstore.books.length, 3);
      for (const book of bookstore.books) {
        assert.ok(book instanceof Book);
        assert.strictEqual(book.bookstore_id, bookstore.id);
      }
    });
  });

  await t.test("m:1 relations", async (t) => {
    await t.test("orm.getAll()", async (t) => {
      const books = await orm.getAll(bookDef, {
        author: bookAuthor()(orm),
      });
      assert.strictEqual(books.length, Object.keys(createdBooks).length);
      for (const book of books) {
        assert.ok(book instanceof Book);
        assert.ok(book.author instanceof Author);
        assert.strictEqual(book.author.id, book.author_id);
      }
    });

    await t.test("orm.getMany()", async (t) => {
      const bookIdsToFetch = [createdBooks.fragileThings.id, createdBooks.guardsGuards.id];
      const books = await orm.getMany(bookDef, qb => qb.where("id", "in", bookIdsToFetch), {
        author: bookAuthor()(orm),
      });
      assert.strictEqual(books.length, bookIdsToFetch.length);
      for (const book of books) {
        assert.ok(book instanceof Book);
        assert.ok(book.author instanceof Author);
        assert.strictEqual(book.author.id, book.author_id);
      }
    });

    await t.test("orm.getOne()", async (t) => {
      const book = await orm.getOne(bookDef, qb => qb.where("id", "=", createdBooks.fragileThings.id), {
        author: bookAuthor()(orm),
      });
      assert.ok(book instanceof Book);
      assert.ok(book.author instanceof Author);
      assert.strictEqual(book.author.id, book.author_id);
    });
  });

  await t.test("Nested relations", async (t) => {
    await t.test("orm.getAll()", async (t) => {
      const bookstores = await orm.getAll(bookstoreDef, {
        books: [bookstoreBooks()(orm), {
          details: bookBookDetails()(orm),
          author: [bookAuthor()(orm), {
            books: authorBooks()(orm),
          }],
        }]
      });
      assert.strictEqual(bookstores.length, Object.keys(createdBookstores).length);
      for (const bookstore of bookstores) {
        assert.ok(bookstore instanceof Bookstore);
        for (const book of bookstore.books) {
          assert.ok(book instanceof Book);
          assert.strictEqual(book.bookstore_id, bookstore.id);
          assert.ok(book.details === null || book.details instanceof BookDetails);
          assert.ok(book.author instanceof Author);
          for (const authorBook of book.author.books) {
            assert.ok(authorBook instanceof Book);
            assert.strictEqual(authorBook.author_id, book.author.id);
          }
        }
      }
    });

    await t.test("orm.getMany()", async (t) => {
      const bookstoreIdsToFetch = [createdBookstores.brigittesBooks.id, createdBookstores.noBooks.id];
      const bookstores = await orm.getMany(bookstoreDef, qb => qb.where("id", "in", bookstoreIdsToFetch), {
        books: [bookstoreBooks()(orm), {
          details: bookBookDetails()(orm),
          author: [bookAuthor()(orm), {
            books: authorBooks()(orm),
          }],
        }]
      });
      assert.strictEqual(bookstores.length, bookstoreIdsToFetch.length);
      for (const bookstore of bookstores) {
        assert.ok(bookstore instanceof Bookstore);
        for (const book of bookstore.books) {
          assert.ok(book instanceof Book);
          assert.strictEqual(book.bookstore_id, bookstore.id);
          assert.ok(book.details === null || book.details instanceof BookDetails);
          assert.ok(book.author instanceof Author);
          for (const authorBook of book.author.books) {
            assert.ok(authorBook instanceof Book);
            assert.strictEqual(authorBook.author_id, book.author.id);
          }
        }
      }
    });

    await t.test("orm.getOne()", async (t) => {
      const bookstore = await orm.getOne(bookstoreDef, qb => qb.where("id", "=", createdBookstores.brigittesBooks.id), {
        books: [bookstoreBooks()(orm), {
          details: bookBookDetails()(orm),
          author: [bookAuthor()(orm), {
            books: authorBooks()(orm),
          }],
        }]
      });
      assert.ok(bookstore instanceof Bookstore);
      assert.strictEqual(bookstore.name, createdBookstores.brigittesBooks.name);
      for (const book of bookstore.books) {
        assert.ok(book instanceof Book);
        assert.strictEqual(book.bookstore_id, bookstore.id);
        assert.ok(book.details === null || book.details instanceof BookDetails);
        assert.ok(book.author instanceof Author);
        for (const authorBook of book.author.books) {
          assert.ok(authorBook instanceof Book);
          assert.strictEqual(authorBook.author_id, book.author.id);
        }
      }
    });
  });

  await t.test("orm.update()", async (t) => {
    await orm.update(bookstoreDef, qb => qb.where("id", "=", createdBookstores.noBooks.id), {
      name: "Some books",
    });

    const changedEntity = await orm.getOne(bookstoreDef, qb => qb.where("id", "=", createdBookstores.noBooks.id));
    assert.ok(changedEntity != null);
    assert.strictEqual(changedEntity.name, "Some books");
  });

  await t.test("orm.transaction()", async (t) => {
    const noBooksStore = await orm.transaction((trxOrm) => {
      return trxOrm.getOne(bookstoreDef, qb => qb.where("id", "=", createdBookstores.noBooks.id));
    });
    assert.strictEqual(noBooksStore!.id, createdBookstores.noBooks.id);
  });

  await t.test("orm.delete()", async (t) => {
    await orm.delete(bookstoreDef, qb => qb.where("id", "=", createdBookstores.noBooks.id));
    const deletedEntity = await orm.getOne(bookstoreDef, qb => qb.where("id", "=", createdBookstores.noBooks.id));
    assert.strictEqual(deletedEntity, null);
  });
});
