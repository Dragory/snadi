import { EntityDefinition, ManyRelationship, MappableOutputType, OneRelationship, RelationsToLoad, WithLoadedRelations, loadRelationsForArray, loadRelationsForEntity, mapArrayToEntity, mapToEntity } from "@snadi/core";
import { Knex } from "knex";

type Optional<T extends object> = {
  [K in keyof T]?: T[K];
};

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
    const rawResults = await this.knex(entityDef.tableName);
    const entities = await mapArrayToEntity(entityDef, rawResults);
    return relations ? loadRelationsForArray(entities, relations) : entities;
  }

  getMany<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Awaited<any>,
  ): Promise<Array<MappableOutputType<EntityDef>>>;
  getMany<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Awaited<any>,
    relations: Relations
  ): Promise<Array<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>>;
  async getMany(entityDef: KnexEntityDefinition, builder: (qb: Knex.QueryBuilder) => Awaited<any>, relations?: RelationsToLoad) {
    const rawResults = await builder(this.knex(entityDef.tableName));
    if (! Array.isArray(rawResults)) {
      throw new Error(`builder for getMany() should return an array`);
    }
    if (rawResults.length === 0) {
      return [];
    }
    const entities = await mapArrayToEntity(entityDef, rawResults);
    return relations ? loadRelationsForArray(entities, relations) : entities;
  }

  getOne<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Awaited<any>,
  ): Promise<MappableOutputType<EntityDef> | null>;
  getOne<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Awaited<any>,
    relations: Relations
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations> | null>;
  async getOne(entityDef: KnexEntityDefinition, builder: (qb: Knex.QueryBuilder) => Awaited<any>, relations?: RelationsToLoad) {
    const rawResult = await builder(this.knex(entityDef.tableName));
    if (Array.isArray(rawResult)) {
      throw new Error(`builder for getOne() should return a single row`);
    }
    if (rawResult == null) {
      return null;
    }
    const entity = await mapToEntity(entityDef, rawResult);
    return relations ? loadRelationsForEntity(entity, relations) : entity;
  }

  async create<EntityDef extends KnexEntityDefinition>(entityDef: EntityDef, data: ToRowInput<EntityDef>): Promise<CreateResult<EntityDef>> {
    const dataToInsert = entityDef.toRow ? await entityDef.toRow(data) : data;
    const inserted = await this.knex(entityDef.tableName).insert(dataToInsert).returning(entityDef.primaryKey ?? "*");
    return (entityDef.primaryKey
      ? this.getOne(entityDef, async qb => await qb.where("id", inserted[0][entityDef.primaryKey!]).first())
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
