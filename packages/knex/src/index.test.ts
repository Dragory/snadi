import test, { after } from "node:test";
import * as knexPkg from "knex";
import { KnexEntityDefinition, KnexOrm, hasMany, hasOne } from "./index.js";
import assert from "node:assert";

const { knex } = knexPkg.default;
test("tests", async (t) => {
  const knexClient = knex({
    client: "sqlite3",
    connection: {
      filename: ":memory:",
    },
    useNullAsDefault: true,
  });

  after(() => {
    knexClient.destroy();
  });

  const orm = new KnexOrm(knexClient);

  // DATABASE TABLES

  await knexClient.schema.createTable("bookstores", (table) => {
    table.increments("id");
    table.string("name");
  });

  await knexClient.schema.createTable("books", (table) => {
    table.increments("id");
    table.string("title");
    table.integer("bookstore_id");
    table.integer("author_id");
  });

  await knexClient.schema.createTable("book_details", (table) => {
    table.increments("id");
    table.integer("book_id").unique();
    table.string("isbn");
  });

  await knexClient.schema.createTable("authors", (table) => {
    table.increments("id");
    table.string("name");
  });

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
    tableName: "bookstores",
    primaryKey: "id",
    toEntity: (data: any) => toEntityClass(Bookstore, data),
  } satisfies KnexEntityDefinition;

  const bookstoreBooks = () => hasMany(bookstoreDef, "id", bookDef, "bookstore_id");

  class Book {
    declare id: number;
    declare title: string;
    declare author_id: number;
    declare bookstore_id: number;
  }

  const bookDef = {
    tableName: "books",
    primaryKey: "id",
    toEntity: (data: any) => toEntityClass(Book, data),
  } satisfies KnexEntityDefinition;

  const bookBookDetails = () => hasOne(bookDef, "id", bookDetailsDef, "book_id");

  const bookAuthor = () => hasOne(bookDef, "author_id", authorDef, "id");

  class BookDetails {
    declare id: number;
    declare book_id: number;
    declare isbn: string;
  };

  const bookDetailsDef = {
    tableName: "book_details",
    primaryKey: "id",
    toEntity: (data: any) => toEntityClass(BookDetails, data),
  } satisfies KnexEntityDefinition;

  class Author {
    declare id: number;
    declare name: string;
  }

  const authorDef = {
    tableName: "authors",
    primaryKey: "id",
    toEntity: (data: any) => toEntityClass(Author, data),
  } satisfies KnexEntityDefinition;

  const authorBooks = () => hasMany(authorDef, "id", bookDef, "author_id");

  // CREATE TEST DATA

  const createdAuthors = {
    neil: await orm.create(authorDef, {
      name: "Neil Gaiman",
    }),
    pratchett: await orm.create(authorDef, {
      name: "Terry Pratchett",
    }),
  };

  const createdBookstores = {
    yeOldeBookShoppe: await orm.create(bookstoreDef, {
      name: "Ye Olde Book Shoppe",
    }),
    brigittesBooks: await orm.create(bookstoreDef, {
      name: "Brigitte's Books",
    }),
    noBooks: await orm.create(bookstoreDef, {
      name: "No Books Store",
    }),
  };

  const createdBooks = {
    fragileThings: await orm.create(bookDef, {
      author_id: createdAuthors.neil.id,
      bookstore_id: createdBookstores.brigittesBooks.id,
      title: "Fragile Things",
    }),
    theColourOfMagic: await orm.create(bookDef, {
      author_id: createdAuthors.pratchett.id,
      bookstore_id: createdBookstores.yeOldeBookShoppe.id,
      title: "The Colour of Magic",
    }),
    guardsGuards: await orm.create(bookDef, {
      author_id: createdAuthors.pratchett.id,
      bookstore_id: createdBookstores.brigittesBooks.id,
      title: "Guards! Guards!",
    }),
    theLastHero: await orm.create(bookDef, {
      author_id: createdAuthors.pratchett.id,
      bookstore_id: createdBookstores.brigittesBooks.id,
      title: "The Last Hero",
    }),
  };

  const createdBookDetails = {
    fragileThings: await orm.create(bookDetailsDef, {
      book_id: createdBooks.fragileThings.id,
      isbn: "0-06-051522-8",
    }),
    theColourOfMagic: await orm.create(bookDetailsDef, {
      book_id: createdBooks.theColourOfMagic.id,
      isbn: "0-86140-324-X",
    }),
  };

  await t.test("orm.create()", async (t) => {
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
      const books = await orm.getMany(bookDef, qb => qb.whereIn("id", bookIdsToFetch), {
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
      const fragileThings = await orm.getOne(bookDef, qb => qb.where("id", createdBooks.fragileThings.id).first(), {
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
      const bookstores = await orm.getMany(bookstoreDef, qb => qb.whereIn("id", bookstoreIdsToFetch), {
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
      const bookstore = await orm.getOne(bookstoreDef, qb => qb.where("id", createdBookstores.brigittesBooks.id).first(), {
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
      const books = await orm.getMany(bookDef, qb => qb.whereIn("id", bookIdsToFetch), {
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
      const book = await orm.getOne(bookDef, qb => qb.where("id", createdBooks.fragileThings.id).first(), {
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
      const bookstores = await orm.getMany(bookstoreDef, qb => qb.whereIn("id", bookstoreIdsToFetch), {
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
      const bookstore = await orm.getOne(bookstoreDef, qb => qb.where("id", createdBookstores.brigittesBooks.id).first(), {
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

  await t.test("Query optimization", async (t) => {
    let queries = 0;
    const listener = () => queries++;
    knexClient.on("query", listener);
    const bookstores = await orm.getAll(bookstoreDef, {
      books: [bookstoreBooks()(orm), {
        details: bookBookDetails()(orm),
        author: [bookAuthor()(orm), {
          books: authorBooks()(orm),
        }],
      }]
    });
    assert.strictEqual(queries, 5);
    knexClient.off("query", listener);
  });
});
