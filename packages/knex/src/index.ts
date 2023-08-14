import { EntityDefinition, ManyRelationship, MappableOutputType, OneRelationship, RelationsToLoad, WithLoadedRelations, loadRelationsForArray, loadRelationsForEntity, mapArrayToEntity, mapToEntity } from "@snadi/core";
import { Knex } from "knex";

export type KnexEntityDefinition = EntityDefinition & {
  tableName: string;
  primaryKey?: string;
};

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
    const entities = await mapArrayToEntity(entityDef, rawResults);
    return relations ? loadRelationsForArray(entities, relations) : entities;
  }

  getOne<EntityDef extends KnexEntityDefinition>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Awaited<any>,
  ): Promise<MappableOutputType<EntityDef>>;
  getOne<EntityDef extends KnexEntityDefinition, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    builder: (qb: Knex.QueryBuilder) => Awaited<any>,
    relations: Relations
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>;
  async getOne(entityDef: KnexEntityDefinition, builder: (qb: Knex.QueryBuilder) => Awaited<any>, relations?: RelationsToLoad) {
    const rawResult = await builder(this.knex(entityDef.tableName));
    if (Array.isArray(rawResult)) {
      throw new Error(`builder for getOne() should return a single row`);
    }
    const entity = await mapToEntity(entityDef, rawResult);
    return relations ? loadRelationsForEntity(entity, relations) : entity;
  }

  async create<EntityDef extends KnexEntityDefinition>(entityDef: EntityDef, data: object): Promise<CreateResult<EntityDef>> {
    const inserted = await this.knex(entityDef.tableName).insert(data).returning(entityDef.primaryKey ?? "*");
    return (entityDef.primaryKey
      ? this.getOne(entityDef, async qb => await qb.where("id", inserted[0][entityDef.primaryKey!]).first())
      : null) as CreateResult<EntityDef>;
  }

  async createMany<EntityDef extends KnexEntityDefinition>(entityDef: EntityDef, arr: object[]): Promise<void> {
    await this.knex(entityDef.tableName).insert(arr);
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
