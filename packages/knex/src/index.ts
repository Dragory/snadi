import { EntityDefinition, ManyRelationship, MappableInputType, MappableOutputType, OneRelationship, RelationsToLoad, WithLoadedRelations, loadRelationsForArray, loadRelationsForEntity, mapArrayToEntity, mapToEntity } from "@snadi/core";
import { Knex } from "knex";

type Optional<T extends object> = {
  [K in keyof T]?: T[K];
};

type Awaitable<T> = T | Promise<T>;

export type KnexEntityDefinition = EntityDefinition & {
  tableName: string;
  primaryKey?: string;

  toRow?: (data: any) => any;
};

type ToRowInput<EntityDef extends KnexEntityDefinition> = EntityDef["toRow"] extends (data: any) => any ? Parameters<EntityDef["toRow"]>[0] : any;

type CreateResult<EntityDef extends KnexEntityDefinition> = EntityDef["primaryKey"] extends string ? MappableOutputType<EntityDef> : null;

export class KnexOrm {
  public knex: Knex;

  constructor(knex: Knex) {
    this.knex = knex;
  }

  getAll<EntityDef extends KnexEntityDefinition>(entityDef: EntityDef): Promise<Array<MappableOutputType<EntityDef>>>;
  getAll<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(entityDef: EntityDef, relations: Relations): Promise<Array<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>>;
  async getAll(entityDef: KnexEntityDefinition, relations?: RelationsToLoad) {
    return this.loadMany(
      entityDef,
      this.knex(entityDef.tableName),
      relations as RelationsToLoad,
    );
  }

  getMany<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder,
  ): Promise<Array<MappableOutputType<EntityDef>>>;
  getMany<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder,
    relations: Relations
  ): Promise<Array<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>>;
  async getMany(entityDef: KnexEntityDefinition, builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder, relations?: RelationsToLoad) {
    return this.loadMany(
      entityDef,
      builder(this.knex(entityDef.tableName)),
      relations as RelationsToLoad,
    );
  }

  getOne<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder,
  ): Promise<MappableOutputType<EntityDef> | null>;
  getOne<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder,
    relations: Relations
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations> | null>;
  async getOne(entityDef: KnexEntityDefinition, builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder, relations?: RelationsToLoad) {
    return this.loadOne(
      entityDef,
      builder(this.knex(entityDef.tableName)).first(),
      relations as RelationsToLoad,
    );
  }

  async create<EntityDef extends KnexEntityDefinition>(entityDef: EntityDef, data: ToRowInput<EntityDef>): Promise<CreateResult<EntityDef>> {
    const dataToInsert = entityDef.toRow ? await entityDef.toRow(data) : data;
    const inserted = await this.knex(entityDef.tableName).insert(dataToInsert).returning(entityDef.primaryKey ?? "*");
    return (entityDef.primaryKey
      ? this.getOne(entityDef, qb => qb.where("id", inserted[0][entityDef.primaryKey!]))
      : null) as CreateResult<EntityDef>;
  }

  async createMany<EntityDef extends KnexEntityDefinition>(entityDef: EntityDef, arr: Array<ToRowInput<EntityDef>>): Promise<void> {
    const arrayToInsert = entityDef.toRow
      ? await Promise.all(arr.map(data => entityDef.toRow!(data)))
      : arr;
    await this.knex(entityDef.tableName).insert(arrayToInsert);
  }

  async update<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder,
    fieldsToUpdate: Optional<ToRowInput<EntityDef>>,
  ): Promise<void> {
    const qb = builder(this.knex(entityDef.tableName));
    await qb.update(fieldsToUpdate);
  }

  async delete<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Knex.QueryBuilder,
  ): Promise<void> {
    const qb = builder(this.knex(entityDef.tableName));
    await qb.delete();
  }

  loadOne<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef> | null>,
  ): Promise<MappableOutputType<EntityDef> | null>;
  loadOne<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef>>,
  ): Promise<MappableOutputType<EntityDef>>;
  loadOne<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef> | null>,
    relations: Relations,
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations> | null>;
  loadOne<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef>>,
    relations: Relations,
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>;
  async loadOne(
    entityDef: KnexEntityDefinition,
    promise: Awaitable<any>,
    relations?: RelationsToLoad,
  ) {
    const row = await promise;
    if (Array.isArray(row)) {
      throw new Error("load function of loadOne() should return a single row, got an array instead");
    }
    if (row == null) {
      return null;
    }
    const entity = await mapToEntity(entityDef, row);
    return relations ? loadRelationsForEntity(entity, relations) : entity;
  }

  loadMany<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    promise: Awaitable<Array<MappableInputType<EntityDef>>>,
  ): Promise<Array<MappableOutputType<EntityDef>>>;
  loadMany<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    promise: Awaitable<Array<MappableInputType<EntityDef>>>,
    relations: RelationsToLoad,
  ): Promise<Array<MappableOutputType<EntityDef>>>;
  async loadMany(
    entityDef: KnexEntityDefinition,
    promise: Awaitable<any>,
    relations?: RelationsToLoad,
  ) {
    const rows = await promise;
    if (! Array.isArray(rows)) {
      throw new Error("load function of loadMany() should return an array of rows, got a non-array instead");
    }
    const entity = await mapArrayToEntity(entityDef, rows);
    return relations ? loadRelationsForArray(entity, relations) : entity;
  }

  async transaction<T>(fn: (orm: KnexOrm) => T, config?: Knex.TransactionConfig): Promise<T> {
    return this.knex.transaction(async (trx) => {
      const trxOrm = new KnexOrm(trx);
      return fn(trxOrm);
    }, config);
  }

  qb<EntityDef extends KnexEntityDefinition>(entityDef: EntityDef): Knex.QueryBuilder {
    return this.knex(entityDef.tableName);
  }
}

export function hasOne<
  LocalEntityDef extends KnexEntityDefinition,
  OtherEntityDef extends KnexEntityDefinition,
>(
  localEntityDef: LocalEntityDef, // Just here for type hints
  localField: keyof MappableOutputType<LocalEntityDef>,
  otherEntityDef: OtherEntityDef,
  otherField: keyof MappableOutputType<OtherEntityDef>,
): (orm: KnexOrm) => OneRelationship<LocalEntityDef, OtherEntityDef> {
  return (orm) => {
    return {
      otherEntity: otherEntityDef,
      load: async (localEntities) => {
        const keys = new Set(localEntities.map(e => e[localField]));
        if (keys.size === 0) {
          return [];
        }
        return orm.knex(otherEntityDef.tableName).whereIn(otherField, Array.from(keys)).select();
      },
      attach: (otherEntities) => {
        const otherEntitiesByKey = new Map<any, MappableOutputType<OtherEntityDef>>();
        for (const otherEntity of otherEntities) {
          otherEntitiesByKey.set(otherEntity[otherField], otherEntity);
        }
        return (localEntity) => {
          return otherEntitiesByKey.get(localEntity[localField]) ?? null;
        };
      },
    };
  };
}

export function hasMany<
  LocalEntityDef extends KnexEntityDefinition,
  OtherEntityDef extends KnexEntityDefinition,
>(
  localEntityDef: LocalEntityDef, // Just here for type hints
  localField: keyof MappableOutputType<LocalEntityDef>,
  otherEntityDef: OtherEntityDef,
  otherField: keyof MappableOutputType<OtherEntityDef>,
): (orm: KnexOrm) => ManyRelationship<LocalEntityDef, OtherEntityDef> {
  return (orm) => {
    return {
      otherEntity: otherEntityDef,
      load: async (localEntities) => {
        const keys = new Set(localEntities.map(e => e[localField]));
        if (keys.size === 0) {
          return [];
        }
        return orm.knex(otherEntityDef.tableName).whereIn(otherField, Array.from(keys)).select();
      },
      attach: (otherEntities) => {
        const otherEntitiesByKey = new Map<any, Array<MappableOutputType<OtherEntityDef>>>();
        for (const otherEntity of otherEntities) {
          if (! otherEntitiesByKey.has(otherEntity[otherField])) {
            otherEntitiesByKey.set(otherEntity[otherField], []);
          }
          otherEntitiesByKey.get(otherEntity[otherField])!.push(otherEntity);
        }
        return (localEntity) => {
          return otherEntitiesByKey.get(localEntity[localField]) ?? [];
        };
      },
    };
  };
}
