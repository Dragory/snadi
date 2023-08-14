import test from "node:test";
import assert from "node:assert";
import { EntityDefinition, Relationship, loadRelationsForArray, mapArrayToEntity } from "./index.js";

// ENTITY: BOOKSTORE

type Bookstore = {
  name: string;
};

const bookstoreEntityDef = {
  toEntity: async (data: Bookstore) => ({ ...data }),
} satisfies EntityDefinition<Bookstore>;

const rawBookstores = [
  { name: "Ye Olde Book Shoppe" },
  { name: "Brigitte's Books" },
];

// 1:m bookstore->books
const bookstoreBooks = () => ({
  otherEntity: bookEntityDef,
  load: async () => [...rawBooks],
  attach: (books) => {
      return (bookstore) => {
          return books.filter(b => b.bookstoreName === bookstore.name);
      };
  },
} satisfies Relationship<typeof bookstoreEntityDef, typeof bookEntityDef>);

// ENTITY: BOOK

type Book = {
  title: string;
  authorName: string;
  bookstoreName: string;
};

const bookEntityDef = {
  toEntity: async (data: Book) => ({ ...data }),
} satisfies EntityDefinition<Book>;

const rawBooks = [
  { title: "Fragile Things", authorName: "Neil Gaiman", bookstoreName: "Brigitte's Books" },
  { title: "The Colour of Magic", authorName: "Terry Pratchett", bookstoreName: "Ye Olde Book Shoppe" },
  { title: "Guards! Guards!", authorName: "Terry Pratchett", bookstoreName: "Brigitte's Books" },
  { title: "The Last Hero", authorName: "Terry Pratchett", bookstoreName: "Brigitte's Books" },
];

// m:1 books->author
const bookAuthor = () => ({
  otherEntity: authorEntityDef,
  load: async () => [...rawAuthors],
  attach:
    (authors) =>
      (book) => authors.find(a => a.name === book.authorName) ?? null,
} satisfies Relationship<typeof bookEntityDef, typeof authorEntityDef>);

// 1:1 book->book details
const bookBookDetails = () => ({
  otherEntity: bookDetailsEntityDef,
  load: async () => [...rawBookDetails],
  attach:
    (bookDetails) =>
      (book) => bookDetails.find(bd => bd.bookTitle === book.title) ?? null,
} satisfies Relationship<typeof bookEntityDef, typeof bookDetailsEntityDef>);

// ENTITY: BOOK DETAILS

type BookDetails = {
  bookTitle: string;
  isbn: string;
};

const bookDetailsEntityDef = {
  toEntity: async (data: BookDetails) => ({ ...data }),
} satisfies EntityDefinition<BookDetails>;

const rawBookDetails = [
  { bookTitle: "Fragile Things", isbn: "0-06-051522-8" },
  { bookTitle: "The Colour of Magic", isbn: "0-86140-324-X" },
];

// ENTITY: AUTHOR

type Author = {
  name: string;
};

const authorEntityDef = {
  toEntity: async (data: Author) => ({ ...data }),
} satisfies EntityDefinition<Author>;

const rawAuthors = [
  { name: "Neil Gaiman" },
  { name: "Terry Pratchett" },
];

const authorBooks = () => ({
  otherEntity: bookEntityDef,
  load: async () => [...rawBooks],
  attach:
    (books) =>
      (author) => books.filter(b => b.authorName === author.name),
} satisfies Relationship<typeof authorEntityDef, typeof bookEntityDef>);

// TEST CASES

test("1:1 relations", async (t) => {
  const books = await mapArrayToEntity(bookEntityDef, rawBooks);
  assert.strictEqual(books.length, 4);

  const booksWithDetails = await loadRelationsForArray(books, {
    details: bookBookDetails(),
  });
  assert.ok("details" in booksWithDetails[0]);

  const exampleBookWithDetails = booksWithDetails.find(b => b.details != null);
  assert.ok(exampleBookWithDetails != null);
  assert.strictEqual(typeof exampleBookWithDetails.details!.isbn, "string");
  assert.strictEqual(exampleBookWithDetails.details!.bookTitle, exampleBookWithDetails.title);
});

test("1:m relations", async (t) => {
  const bookstores = await mapArrayToEntity(bookstoreEntityDef, rawBookstores);
  assert.strictEqual(bookstores.length, 2);

  const bookstoresWithBooks = await loadRelationsForArray(bookstores, {
    books: bookstoreBooks(),
  });
  assert.ok("books" in bookstoresWithBooks[0]);
  assert.ok(Array.isArray(bookstoresWithBooks[0].books));

  const yeOldeBookShoppe = bookstoresWithBooks[0];
  assert.strictEqual(yeOldeBookShoppe.books.length, 1);
  assert.strictEqual(yeOldeBookShoppe.books[0].title, "The Colour of Magic");

  const brigittesBooks = bookstoresWithBooks[1];
  assert.strictEqual(brigittesBooks.books.length, 3);
});

test("m:1 relations", async (t) => {
  const books = await mapArrayToEntity(bookEntityDef, rawBooks);
  assert.strictEqual(books.length, 4);

  const booksWithAuthors = await loadRelationsForArray(books, {
    author: bookAuthor(),
  });
  assert.ok(booksWithAuthors.every(b => ("author" in b)));

  assert.strictEqual(booksWithAuthors[0].author!.name, "Neil Gaiman");
  assert.strictEqual(booksWithAuthors[1].author!.name, "Terry Pratchett");
  assert.strictEqual(booksWithAuthors[2].author!.name, "Terry Pratchett");
  assert.strictEqual(booksWithAuthors[3].author!.name, "Terry Pratchett");
});

test("Nested relations", async (t) => {
  const bookstores = await mapArrayToEntity(bookstoreEntityDef, rawBookstores);
  const bookstoresWithRelations = await loadRelationsForArray(bookstores, {
    books: [bookstoreBooks(), {
      details: bookBookDetails(),
      author: [bookAuthor(), {
        books: authorBooks(),
      }],
    }],
  });

  for (const bookstore of bookstoresWithRelations) {
    assert.ok(Array.isArray(bookstore.books));
    for (const book of bookstore.books) {
      assert.ok("details" in book);
      assert.ok("author" in book);
      assert.ok(Array.isArray(book.author!.books));
    }
  }
});
